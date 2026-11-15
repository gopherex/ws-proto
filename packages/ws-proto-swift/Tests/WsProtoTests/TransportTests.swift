import SwiftProtobuf
import XCTest

@testable import WsProto

final class FakeSocket: WsSocket, @unchecked Sendable {
  let events: AsyncStream<SocketEvent>
  let continuation: AsyncStream<SocketEvent>.Continuation
  private let lock = NSLock()
  private var sent: [Data] = []
  private var stopped = false
  init() {
    var continuation: AsyncStream<SocketEvent>.Continuation!
    events = AsyncStream { continuation = $0 }
    self.continuation = continuation
  }
  func send(_ data: Data) async throws { record(data) }
  private func record(_ data: Data) {
    lock.lock()
    defer { lock.unlock() }
    sent.append(data)
  }
  func close() {
    lock.lock()
    stopped = true
    lock.unlock()
    continuation.finish()
  }
  func open(_ name: String = "wsrpc.v1") { continuation.yield(.opened(protocolName: name)) }
  func inject(_ frame: WireFrame) throws {
    continuation.yield(.message(try frame.serializedData()))
  }
  func frames() throws -> [WireFrame] {
    lock.lock()
    defer { lock.unlock() }
    return try sent.map { try WireFrame(serializedBytes: $0) }
  }
}

final class SocketPool: @unchecked Sendable {
  private let lock = NSLock()
  private var sockets: [FakeSocket]
  init(_ sockets: [FakeSocket]) { self.sockets = sockets }
  func next() -> FakeSocket {
    lock.lock()
    defer { lock.unlock() }
    return sockets.removeFirst()
  }
}

final class TransportTests: XCTestCase {
  actor Counter {
    var value = 0
    func increment() { value += 1 }
  }
  struct TrackingSource: AsyncSequence, Sendable {
    typealias Element = Google_Protobuf_Empty
    let counter: Counter
    struct AsyncIterator: AsyncIteratorProtocol {
      let counter: Counter
      mutating func next() async -> Google_Protobuf_Empty? {
        await counter.increment()
        return nil
      }
    }
    func makeAsyncIterator() -> AsyncIterator { AsyncIterator(counter: counter) }
  }
  func waitFor(_ condition: @escaping () async throws -> Bool) async throws {
    for _ in 0..<1000 {
      if try await condition() { return }
      try await Task.sleep(nanoseconds: 1_000_000)
    }
    XCTFail("condition timed out")
    throw WsStatusError(code: 4, message: "test timeout")
  }
  func wire(
    _ kind: Wsproto_Transport_V1_Kind, id: UInt32 = 1, payload: Data = Data(), code: Int32 = 0,
    headers: [String: String] = [:], window: UInt32 = 0
  ) -> WireFrame {
    var value = WireFrame()
    value.streamID = id
    value.kind = kind
    value.payload = payload
    value.status.code = code
    value.headers = headers
    value.window = window
    return value
  }
  func testUnaryMetadataAndTrailingError() async throws {
    let socket = FakeSocket()
    let transport = WsTransport(socket: socket)
    let request = Google_Protobuf_StringValue.with { $0.value = "hello" }
    let task = Task {
      try await transport.unary(
        method: "/test/Echo", request: request, responseType: Google_Protobuf_StringValue.self)
    }
    socket.open()
    try await waitFor { try socket.frames().count == 3 }
    let frames = try socket.frames()
    XCTAssertEqual(frames.map(\.kind), [.open, .msg, .halfClose])
    try socket.inject(wire(.header, headers: ["header": "one"]))
    try socket.inject(wire(.msg, payload: request.serializedData()))
    try socket.inject(wire(.end, headers: ["trailer": "two"]))
    let result = try await task.value
    XCTAssertEqual(result.message.value, "hello")
    XCTAssertEqual(result.headers, ["header": "one"])
    XCTAssertEqual(result.trailers, ["trailer": "two"])
    let errorTask = Task {
      try await transport.unary(
        method: "/test/Echo", request: request, responseType: Google_Protobuf_StringValue.self)
    }
    try await waitFor { try socket.frames().contains { $0.streamID == 3 && $0.kind == .halfClose } }
    try socket.inject(wire(.msg, id: 3, payload: request.serializedData()))
    try socket.inject(wire(.end, id: 3, code: 7))
    do {
      _ = try await errorTask.value
      XCTFail("expected trailing error")
    } catch let error as WsStatusError { XCTAssertEqual(error.code, 7) }
    await transport.close()
  }

  func testFlowControlDefersHalfCloseUntilBufferedMessages() async throws {
    let socket = FakeSocket()
    var options = TransportOptions()
    options.initialWindow = 1
    let transport = WsTransport(socket: socket, options: options)
    let request = Google_Protobuf_StringValue.with { $0.value = "oversized" }
    let source = AsyncStream<Google_Protobuf_StringValue> {
      $0.yield(request)
      $0.yield(request)
      $0.finish()
    }
    let stream = try await transport.bidiStreaming(
      method: "/test/Bidi", requests: source, responseType: Google_Protobuf_StringValue.self)
    socket.open()
    try await waitFor { try socket.frames().count == 2 }
    try await Task.sleep(nanoseconds: 10_000_000)
    XCTAssertEqual(try socket.frames().map(\.kind), [.open, .msg])
    try socket.inject(wire(.windowUpdate, window: UInt32(try request.serializedData().count)))
    try await waitFor { try socket.frames().count == 4 }
    XCTAssertEqual(try socket.frames().map(\.kind), [.open, .msg, .msg, .halfClose])
    await stream.cancel()
    await transport.close()
  }

  func testDeadlineCancellationAndHeadersAlwaysSettle() async throws {
    let socket = FakeSocket()
    let transport = WsTransport(socket: socket)
    let stream = try await transport.serverStreaming(
      method: "/test/Wait", request: Google_Protobuf_Empty(),
      responseType: Google_Protobuf_Empty.self, options: CallOptions(timeoutMs: 10))
    socket.open()
    do {
      for try await _ in stream {}
      XCTFail("deadline did not fire")
    } catch let error as WsStatusError { XCTAssertEqual(error.code, 4) }
    let headers = await stream.headers()
    let trailers = await stream.trailers()
    XCTAssertEqual(headers, [:])
    XCTAssertEqual(trailers, [:])
    try await waitFor { try socket.frames().contains { $0.kind == .rst } }
    XCTAssertEqual(try socket.frames().first?.headers["ws-timeout-ms"], "10")
    let task = Task {
      try await transport.unary(
        method: "/test/Wait", request: Google_Protobuf_Empty(),
        responseType: Google_Protobuf_Empty.self)
    }
    task.cancel()
    do {
      _ = try await task.value
      XCTFail("cancel did not fail")
    } catch let error as WsStatusError { XCTAssertEqual(error.code, 1) }
    await transport.close()
  }

  func testEarlyServerEndDoesNotWaitForIdleSource() async throws {
    let socket = FakeSocket()
    let transport = WsTransport(socket: socket)
    let source = AsyncStream<Google_Protobuf_Empty> { _ in }
    let stream = try await transport.bidiStreaming(
      method: "/test/Bidi", requests: source, responseType: Google_Protobuf_Empty.self)
    socket.open()
    try await waitFor { try socket.frames().count == 1 }
    try socket.inject(wire(.end))
    for try await _ in stream { XCTFail("unexpected message") }
    let trailers = await stream.trailers()
    XCTAssertEqual(trailers, [:])
    await transport.close()
  }

  func testBufferedResponsesDrainBeforeErrorAndCreditOnConsumption() async throws {
    let socket = FakeSocket()
    var options = TransportOptions()
    options.initialWindow = 4
    let transport = WsTransport(socket: socket, options: options)
    let stream = try await transport.serverStreaming(
      method: "/test/Stream", request: Google_Protobuf_Empty(),
      responseType: Google_Protobuf_StringValue.self)
    socket.open()
    try await waitFor { try socket.frames().count == 3 }
    let message = Google_Protobuf_StringValue.with { $0.value = "ab" }
    try socket.inject(wire(.msg, payload: message.serializedData()))
    try await Task.sleep(nanoseconds: 10_000_000)
    XCTAssertFalse(try socket.frames().contains { $0.kind == .windowUpdate })
    let iterator = stream.makeAsyncIterator()
    let first = try await iterator.next()
    XCTAssertEqual(first?.value, "ab")
    try await waitFor { try socket.frames().contains { $0.kind == .windowUpdate } }
    try socket.inject(wire(.msg, payload: message.serializedData()))
    try socket.inject(wire(.end, code: 3))
    let second = try await iterator.next()
    XCTAssertEqual(second?.value, "ab")
    do {
      _ = try await iterator.next()
      XCTFail("expected error")
    } catch let error as WsStatusError { XCTAssertEqual(error.code, 3) }
    await transport.close()
  }

  func testPermanentFailuresFailFutureCalls() async throws {
    for mode in 0..<3 {
      let socket = FakeSocket()
      var options = TransportOptions()
      options.maxFrameBytes = mode == 2 ? 1 : 0
      let transport = WsTransport(socket: socket, options: options)
      await transport.connect()
      socket.open(mode == 0 ? "wrong" : "wsrpc.v1")
      if mode == 1 { try socket.inject(wire(.connEnd, id: 0, code: 7)) }
      if mode == 2 { socket.continuation.yield(.message(Data([0, 0]))) }
      try await waitFor {
        if case .disconnected = await transport.state { return true }
        return false
      }
      do {
        _ = try await transport.unary(
          method: "/test/X", request: Google_Protobuf_Empty(),
          responseType: Google_Protobuf_Empty.self)
        XCTFail("expected failure")
      } catch let error as WsStatusError {
        XCTAssertEqual(error.code, mode == 0 ? 14 : mode == 1 ? 7 : 8)
      }
      await transport.close()
    }
  }

  func testPausedDialAndRetryControls() async throws {
    let socket = FakeSocket()
    var options = TransportOptions()
    options.paused = true
    options.reconnect = true
    let transport = WsTransport(
      url: URL(string: "ws://localhost")!, options: options, createSocket: { _ in socket })
    let initial = await transport.state
    XCTAssertEqual(initial, .paused)
    await transport.retryNow()
    let stillPaused = await transport.state
    XCTAssertEqual(stillPaused, .paused)
    await transport.resume()
    socket.open()
    try await waitFor { await transport.state == .connected }
    await transport.pause()
    let connected = await transport.state
    XCTAssertEqual(connected, .connected)
    socket.continuation.yield(.closed)
    try await waitFor { await transport.state == .paused }
    await transport.close()
    await transport.resume()
    let closed = await transport.state
    XCTAssertEqual(closed, .closed)
  }

  func testHalfCloseIsSingleAndSuppressesLateSendsAndTerminalWrites() async throws {
    for terminal in [false, true] {
      let socket = FakeSocket()
      var options = TransportOptions()
      options.initialWindow = 1
      let transport = WsTransport(socket: socket, options: options)
      let call = await transport.openStream(method: "/test/Raw")
      socket.open()
      await call.send(Data([1]))
      await call.send(Data([2]))
      await call.closeSend()
      await call.closeSend()
      await call.send(Data([3]))
      try await waitFor { try socket.frames().count == 2 }
      if terminal { await call.cancel() }
      try socket.inject(wire(.windowUpdate, window: 1))
      try await waitFor { try socket.frames().count == (terminal ? 3 : 4) }
      XCTAssertEqual(try socket.frames().filter { $0.kind == .msg }.count, terminal ? 1 : 2)
      XCTAssertEqual(try socket.frames().filter { $0.kind == .halfClose }.count, terminal ? 0 : 1)
      await transport.close()
    }
  }

  func testSendAndReceiveOverflowIsIsolatedToCall() async throws {
    for receiving in [false, true] {
      let socket = FakeSocket()
      var options = TransportOptions()
      options.initialWindow = 1
      options.maxSendBuffer = 1
      let transport = WsTransport(socket: socket, options: options)
      let call = await transport.openStream(method: "/test/Raw")
      socket.open()
      try await waitFor { try socket.frames().count == 1 }
      if receiving {
        try socket.inject(wire(.msg, payload: Data([1, 2])))
        try socket.inject(wire(.msg, payload: Data([3])))
      } else {
        await call.send(Data([1]))
        await call.send(Data([2, 3]))
        await call.send(Data([4]))
      }
      try await waitFor { try socket.frames().contains { $0.kind == .rst } }
      do {
        while try await call.recv() != nil {}
        XCTFail("expected resource exhaustion")
      } catch let error as WsStatusError { XCTAssertEqual(error.code, 8) }
      let next = await transport.openStream(method: "/test/Next")
      try await waitFor { try socket.frames().contains { $0.kind == .open && $0.streamID == 3 } }
      await next.cancel()
      await transport.close()
    }
  }

  func testPreconnectOverflowIsPermanent() async throws {
    let socket = FakeSocket()
    let transport = WsTransport(socket: socket)
    let call = await transport.openStream(method: "/test/Raw")
    await call.send(Data(repeating: 1, count: 4 * 1024 * 1024))
    do {
      _ = try await call.recv()
      XCTFail("expected preconnect overflow")
    } catch let error as WsStatusError { XCTAssertEqual(error.code, 14) }
    let state = await transport.state
    guard case .disconnected = state else { return XCTFail("expected disconnected") }
    await transport.close()
  }

  func testDialTimeoutAndPauseDuringHandshake() async throws {
    for pausing in [false, true] {
      let socket = FakeSocket()
      var options = TransportOptions()
      options.dialTimeoutMs = 10
      let transport = WsTransport(
        url: URL(string: "ws://localhost")!, options: options, createSocket: { _ in socket })
      let call = await transport.openStream(method: "/test/Raw")
      if pausing { await transport.pause() }
      do {
        _ = try await call.recv()
        XCTFail("expected dial failure")
      } catch let error as WsStatusError { XCTAssertEqual(error.code, 14) }
      if pausing {
        let state = await transport.state
        XCTAssertEqual(state, .paused)
      }
      await transport.close()
    }
  }

  func testUnaryEmptyAndExtraResponseAreRejected() async throws {
    for count in [0, 2] {
      let socket = FakeSocket()
      let transport = WsTransport(socket: socket)
      let result = Task {
        try await transport.unary(
          method: "/test/U", request: Google_Protobuf_Empty(),
          responseType: Google_Protobuf_Empty.self)
      }
      socket.open()
      try await waitFor { try socket.frames().count == 3 }
      for _ in 0..<count { try socket.inject(wire(.msg)) }
      try socket.inject(wire(.end))
      do {
        _ = try await result.value
        XCTFail("expected cardinality failure")
      } catch let error as WsStatusError { XCTAssertEqual(error.code, 13) }
      await transport.close()
    }
  }

  struct TransformInterceptor: ClientInterceptor {
    func intercept(_ request: RPCRequest, next: @escaping RPCHandler) async throws -> RPCResponse {
      var request = request
      request.options.headers["intercepted"] = "yes"
      request.messages = request.messages.mapMessages { message in
        var value = message as! Google_Protobuf_StringValue
        value.value += " request"
        return value
      }
      var response = try await next(request)
      response.messages = response.messages.mapMessages { message in
        var value = message as! Google_Protobuf_StringValue
        value.value += " response"
        return value
      }
      return response
    }
  }
  func testInterceptorTransformsMessagesAndMetadata() async throws {
    let socket = FakeSocket()
    var options = TransportOptions()
    options.interceptors = [TransformInterceptor()]
    let transport = WsTransport(socket: socket, options: options)
    let request = Google_Protobuf_StringValue.with { $0.value = "hello" }
    let task = Task {
      try await transport.unary(
        method: "/test/U", request: request, responseType: Google_Protobuf_StringValue.self)
    }
    socket.open()
    try await waitFor { try socket.frames().count == 3 }
    let sent = try socket.frames()
    XCTAssertEqual(sent[0].headers["intercepted"], "yes")
    let body = try Google_Protobuf_StringValue(serializedBytes: sent[1].payload)
    XCTAssertEqual(body.value, "hello request")
    try socket.inject(wire(.msg, payload: request.serializedData()))
    try socket.inject(wire(.end))
    let result = try await task.value
    XCTAssertEqual(result.message.value, "hello response")
    await transport.close()
  }

  func testBreakingIterationCancelsStream() async throws {
    let socket = FakeSocket()
    let transport = WsTransport(socket: socket)
    let stream = try await transport.serverStreaming(
      method: "/test/S", request: Google_Protobuf_Empty(), responseType: Google_Protobuf_Empty.self)
    socket.open()
    try await waitFor { try socket.frames().count == 3 }
    try socket.inject(wire(.msg))
    for try await _ in stream { break }
    try await waitFor { try socket.frames().contains { $0.kind == .rst } }
    await transport.close()
  }

  func testCancellationWhileWaitingForMetadataStopsIdleSource() async throws {
    let socket = FakeSocket()
    let transport = WsTransport(socket: socket)
    let source = AsyncStream<Google_Protobuf_Empty> { _ in }
    let stream = try await transport.bidiStreaming(
      method: "/test/S", requests: source, responseType: Google_Protobuf_Empty.self)
    socket.open()
    try await waitFor { try socket.frames().count == 1 }
    let task = Task { await stream.headers() }
    task.cancel()
    let header = await task.value
    XCTAssertEqual(header, [:])
    try await waitFor { try socket.frames().contains { $0.kind == .rst } }
    do {
      for try await _ in stream {}
      XCTFail("expected cancellation")
    } catch let error as WsStatusError { XCTAssertEqual(error.code, 1) }
    await transport.close()
  }

  func testReconnectFailsExistingCallAndNewGenerationStartsAtOne() async throws {
    for automatic in [false, true] {
      let first = FakeSocket()
      let second = FakeSocket()
      let pool = SocketPool([first, second])
      var options = TransportOptions()
      options.reconnect = automatic
      options.initialBackoffMs = 1
      options.maxBackoffMs = 1
      let transport = WsTransport(
        url: URL(string: "ws://localhost")!, options: options, createSocket: { _ in pool.next() })
      let call = await transport.openStream(method: "/test/S")
      first.open()
      try await waitFor { try first.frames().count == 1 }
      first.continuation.yield(.closed)
      do {
        _ = try await call.recv()
        XCTFail("expected disconnect")
      } catch let error as WsStatusError { XCTAssertEqual(error.code, 14) }
      if !automatic { await transport.retryNow() }
      second.open()
      try await waitFor { await transport.state == .connected }
      let next = await transport.openStream(method: "/test/New")
      try await waitFor { try second.frames().count == 1 }
      XCTAssertEqual(try second.frames()[0].streamID, 1)
      await next.cancel()
      await transport.close()
    }
  }

  func testFailedAtBirthDoesNotStartRequestSource() async throws {
    for closed in [false, true] {
      let counter = Counter()
      var options = TransportOptions()
      options.paused = true
      let transport = WsTransport(url: URL(string: "ws://localhost")!, options: options)
      if closed { await transport.close() }
      let stream = try await transport.bidiStreaming(
        method: "/test/S", requests: TrackingSource(counter: counter),
        responseType: Google_Protobuf_Empty.self)
      do {
        for try await _ in stream {}
        XCTFail("expected unavailable")
      } catch let error as WsStatusError { XCTAssertEqual(error.code, closed ? 1 : 14) }
      try await Task.sleep(nanoseconds: 10_000_000)
      let count = await counter.value
      XCTAssertEqual(count, 0)
      await transport.close()
    }
  }

  func testClosedStateSubscriptionCompletes() async {
    let transport = WsTransport(socket: FakeSocket())
    await transport.close()
    var values: [ConnectionState] = []
    for await state in await transport.states() { values.append(state) }
    XCTAssertEqual(values, [.closed])
  }

  func testAlreadyCancelledTaskSendsNoRequest() async throws {
    let socket = FakeSocket()
    let transport = WsTransport(socket: socket)
    socket.open()
    let task = Task {
      withUnsafeCurrentTask { $0?.cancel() }
      return try await transport.unary(
        method: "/test/U", request: Google_Protobuf_Empty(),
        responseType: Google_Protobuf_Empty.self)
    }
    do {
      _ = try await task.value
      XCTFail("expected cancellation")
    } catch let error as WsStatusError { XCTAssertEqual(error.code, 1) }
    XCTAssertEqual(try socket.frames().count, 0)
    await transport.close()
  }
}
