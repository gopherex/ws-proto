import Foundation
import SwiftProtobuf

typealias WireFrame = Wsproto_Transport_V1_Frame

/// All multiplexing state is isolated on this actor. A single writer preserves frame order.
public actor WsTransport {
  public typealias SocketFactory = @Sendable (URL) -> any WsSocket
  final class Call: @unchecked Sendable {
    let id: UInt32
    let generation: Int
    let options: CallOptions
    var inbound: [Data] = []
    var inboundBytes = 0
    var outbound: [Data] = []
    var outboundBytes = 0
    var sendWindow: Int
    var pendingCredit = 0
    var sendClosed = false
    var halfCloseSent = false
    var finished = false
    var error: Error?
    var leading: [String: String]?
    var trailer: [String: String]?
    var readers: [CheckedContinuation<Data?, Error>] = []
    var headerWaiters: [CheckedContinuation<[String: String], Never>] = []
    var trailerWaiters: [CheckedContinuation<[String: String], Never>] = []
    var deadline: Task<Void, Never>?
    var pump: Task<Void, Never>?
    init(id: UInt32, generation: Int, window: Int, options: CallOptions) {
      self.id = id
      self.generation = generation
      self.sendWindow = window
      self.options = options
    }
  }

  private let url: URL?
  private let factory: SocketFactory?
  private let options: TransportOptions
  private let initialWindow: Int
  private let maxSendBuffer: Int
  private var socket: (any WsSocket)?
  private var generation = 0
  private var nextID: UInt64 = 1
  private var calls: [UInt32: Call] = [:]
  private var frames: [Data] = []
  private var frameBytes = 0
  private var writing = false
  private var live = false
  private var terminal: WsStatusError?
  private var paused: Bool
  private var closed = false
  private var attempt = 0
  private var reader: Task<Void, Never>?
  private var retry: Task<Void, Never>?
  private var dialTimer: Task<Void, Never>?
  private var stableTimer: Task<Void, Never>?
  private var observers: [UUID: AsyncStream<ConnectionState>.Continuation] = [:]
  public private(set) var state: ConnectionState

  public init(url: URL, options: TransportOptions = .init(), createSocket: SocketFactory? = nil) {
    self.url = url
    self.options = options
    self.factory = createSocket ?? { URLSessionSocket(url: $0, headers: options.headers) }
    initialWindow = options.initialWindow > 0 ? options.initialWindow : 256 * 1024
    maxSendBuffer = options.maxSendBuffer > 0 ? options.maxSendBuffer : 16 * 1024 * 1024
    paused = options.paused
    state = options.paused ? .paused : .connecting
  }

  public init(socket: any WsSocket, options: TransportOptions = .init()) {
    url = nil
    factory = nil
    self.socket = socket
    self.options = options
    initialWindow = options.initialWindow > 0 ? options.initialWindow : 256 * 1024
    maxSendBuffer = options.maxSendBuffer > 0 ? options.maxSendBuffer : 16 * 1024 * 1024
    paused = false
    state = .connecting
  }

  deinit {
    reader?.cancel()
    retry?.cancel()
    dialTimer?.cancel()
    stableTimer?.cancel()
    socket?.close()
    for continuation in observers.values { continuation.finish() }
  }

  /// Starts dialing. Calls and state subscriptions also start the transport lazily.
  public func connect() {
    guard !closed, !paused, reader == nil, terminal == nil else { return }
    if let socket { attach(socket) } else { dial() }
  }
  public func states() -> AsyncStream<ConnectionState> {
    connect()
    let id = UUID()
    return AsyncStream { continuation in
      continuation.yield(state)
      if closed {
        continuation.finish()
        return
      }
      observers[id] = continuation
      continuation.onTermination = { [weak self] _ in Task { await self?.removeObserver(id) } }
    }
  }
  private func removeObserver(_ id: UUID) { observers.removeValue(forKey: id) }
  private func setState(_ value: ConnectionState) {
    state = value
    for continuation in observers.values { continuation.yield(value) }
  }
  private func dial() {
    guard !closed, !paused, let url, let factory else { return }
    retry?.cancel()
    retry = nil
    attach(factory(url))
  }
  private func attach(_ socket: any WsSocket) {
    generation += 1
    let current = generation
    self.socket = socket
    terminal = nil
    live = false
    nextID = 1
    setState(.connecting)
    reader = Task { [weak self] in
      for await event in socket.events { await self?.receive(event, generation: current) }
      await self?.receive(.closed, generation: current)
    }
    if options.dialTimeoutMs > 0 {
      dialTimer = timer(ms: options.dialTimeoutMs) { [weak self] in
        await self?.dialExpired(current)
      }
    }
  }
  private func dialExpired(_ current: Int) {
    guard current == generation, !live, terminal == nil else { return }
    drop(WsStatusError(code: 14, message: "dial timeout"), reconnect: true)
  }
  private func timer(ms: Int, action: @escaping @Sendable () async -> Void) -> Task<Void, Never> {
    Task {
      let milliseconds = UInt64(clamping: max(0, ms))
      let nanoseconds =
        milliseconds > UInt64.max / 1_000_000 ? UInt64.max : milliseconds * 1_000_000
      do { try await Task.sleep(nanoseconds: nanoseconds) } catch { return }
      guard !Task.isCancelled else { return }
      await action()
    }
  }
  private func receive(_ event: SocketEvent, generation current: Int) {
    guard current == generation, !closed, terminal == nil else { return }
    switch event {
    case .opened(let selected):
      guard selected == "wsrpc.v1" else {
        drop(
          WsStatusError(code: 14, message: "server did not negotiate subprotocol wsrpc.v1"),
          reconnect: false)
        return
      }
      guard !live else { return }
      live = true
      dialTimer?.cancel()
      dialTimer = nil
      if options.stableMs <= 0 {
        attempt = 0
      } else {
        stableTimer = timer(ms: options.stableMs) { [weak self] in await self?.resetAttempt(current)
        }
      }
      setState(.connected)
      flush()
    case .closed: drop(WsStatusError(code: 14, message: "connection lost"), reconnect: true)
    case .message(let data):
      if options.maxFrameBytes > 0, data.count > options.maxFrameBytes {
        drop(
          WsStatusError(code: 8, message: "inbound frame exceeds maxFrameBytes"), reconnect: false)
        return
      }
      guard let frame = try? WireFrame(serializedBytes: data) else { return }
      handle(frame)
    }
  }
  private func resetAttempt(_ current: Int) { if current == generation, live { attempt = 0 } }
  private func drop(_ error: WsStatusError, reconnect: Bool) {
    guard terminal == nil else { return }
    terminal = error
    live = false
    dialTimer?.cancel()
    stableTimer?.cancel()
    reader?.cancel()
    reader = nil
    socket?.close()
    frames.removeAll()
    frameBytes = 0
    writing = false
    for call in Array(calls.values) { finish(call, error: error) }
    if closed { return }
    guard reconnect, options.reconnect, url != nil else {
      setState(.disconnected(error))
      return
    }
    if paused {
      setState(.paused)
      return
    }
    let cap = max(1, options.maxBackoffMs)
    var delay = min(max(1, options.initialBackoffMs), cap)
    for _ in 0..<min(attempt, 63) {
      if delay >= cap { break }
      delay = min(delay > cap / 2 ? cap : delay * 2, cap)
    }
    attempt += 1
    delay = Int.random(in: 0...delay)
    setState(.waiting(retryAt: Date().addingTimeInterval(Double(delay) / 1000)))
    let current = generation
    retry = timer(ms: delay) { [weak self] in await self?.retryExpired(current) }
  }
  private func retryExpired(_ current: Int) {
    guard current == generation, !Task.isCancelled else { return }
    guard case .waiting = state else { return }
    dial()
  }
  public func pause() {
    guard !closed, url != nil, !paused else { return }
    paused = true
    retry?.cancel()
    retry = nil
    switch state {
    case .connecting:
      drop(WsStatusError(code: 14, message: "dial paused"), reconnect: false)
      setState(.paused)
    case .waiting: setState(.paused)
    default: break
    }
  }
  public func resume() {
    guard !closed, paused else { return }
    paused = false
    attempt = 0
    if state == .paused { dial() }
  }
  public func retryNow() {
    guard !closed, !paused, url != nil else { return }
    switch state {
    case .waiting, .disconnected: dial()
    default: break
    }
  }
  public func close() {
    guard !closed else { return }
    closed = true
    retry?.cancel()
    dialTimer?.cancel()
    stableTimer?.cancel()
    let error = WsStatusError(code: 1, message: "transport closed")
    terminal = nil
    drop(error, reconnect: false)
    setState(.closed)
    for continuation in observers.values { continuation.finish() }
    observers.removeAll()
  }

  private func write(_ frame: WireFrame) {
    guard terminal == nil, !closed else { return }
    guard let bytes = try? frame.serializedData() else { return }
    if !live && frameBytes + bytes.count > 4 * 1024 * 1024 {
      drop(
        WsStatusError(code: 14, message: "send buffer overflow before connect"), reconnect: false)
      return
    }
    frames.append(bytes)
    frameBytes += bytes.count
    flush()
  }
  private func flush() {
    guard live, !writing, terminal == nil, let socket else { return }
    writing = true
    let current = generation
    Task { [weak self] in
      while let bytes = await self?.nextFrame(current) {
        do { try await socket.send(bytes) } catch {
          await self?.sendFailed(current)
          return
        }
      }
    }
  }
  private func nextFrame(_ current: Int) -> Data? {
    guard current == generation else { return nil }
    guard live, terminal == nil, !frames.isEmpty else {
      writing = false
      return nil
    }
    let data = frames.removeFirst()
    frameBytes -= data.count
    return data
  }
  private func sendFailed(_ current: Int) {
    if current == generation {
      writing = false
      drop(WsStatusError(code: 14, message: "connection lost"), reconnect: true)
    }
  }
  private func frame(
    _ call: Call, _ kind: Wsproto_Transport_V1_Kind, payload: Data = Data(), window: Int = 0
  ) {
    guard call.generation == generation else { return }
    var value = WireFrame()
    value.streamID = call.id
    value.kind = kind
    value.payload = payload
    value.window = UInt32(clamping: window)
    write(value)
  }
  public func openStream(method: String, options: CallOptions = .init()) -> ClientStream {
    let call = open(method, options: options)
    return ClientStream(
      send: { await self.send(call, payload: $0) },
      closeSend: { await self.closeSend(call) },
      recv: { try await self.pull(call) },
      headers: { await self.headers(call) },
      trailers: { await self.trailers(call) },
      cancel: { await self.abort(call) })
  }
  private func open(_ method: String, options: CallOptions) -> Call {
    connect()
    let call = Call(
      id: UInt32(clamping: nextID), generation: generation, window: initialWindow, options: options)
    if closed {
      finish(call, error: WsStatusError(code: 1, message: "transport closed"))
      return call
    }
    if let terminal {
      finish(call, error: terminal)
      return call
    }
    if Task.isCancelled {
      finish(call, error: WsStatusError(code: 1, message: "cancelled"))
      return call
    }
    if paused && !live {
      finish(call, error: WsStatusError(code: 14, message: "not connected"))
      return call
    }
    guard nextID <= UInt32.max else {
      finish(call, error: WsStatusError(code: 8, message: "stream ids exhausted"))
      return call
    }
    nextID += 2
    calls[call.id] = call
    var value = WireFrame()
    value.streamID = call.id
    value.kind = .open
    value.method = method
    value.headers = options.headers
    if options.timeoutMs > 0 { value.headers["ws-timeout-ms"] = String(options.timeoutMs) }
    write(value)
    if options.timeoutMs > 0, !call.finished {
      call.deadline = timer(ms: options.timeoutMs) { [weak self, weak call] in
        if let call {
          await self?.abort(call, error: WsStatusError(code: 4, message: "deadline exceeded"))
        }
      }
    }
    return call
  }
  private func send(_ call: Call, payload: Data) {
    guard !call.finished, !call.sendClosed else { return }
    if call.outboundBytes > maxSendBuffer {
      abort(call, error: WsStatusError(code: 8, message: "send buffer overflow"))
      return
    }
    call.outbound.append(payload)
    call.outboundBytes += payload.count
    pump(call)
  }
  private func pump(_ call: Call) {
    while !call.outbound.isEmpty, call.sendWindow > 0, !call.finished {
      let data = call.outbound.removeFirst()
      call.outboundBytes -= data.count
      call.sendWindow -= data.count
      frame(call, .msg, payload: data)
    }
    if call.sendClosed, call.outbound.isEmpty, !call.halfCloseSent, !call.finished {
      call.halfCloseSent = true
      frame(call, .halfClose)
    }
  }
  private func closeSend(_ call: Call) {
    guard !call.finished else { return }
    call.sendClosed = true
    pump(call)
  }
  private func abort(_ call: Call, error: Error? = nil) {
    guard !call.finished else { return }
    frame(call, .rst)
    finish(call, error: error)
  }
  private func leading(_ call: Call, _ headers: [String: String]) {
    guard call.leading == nil else { return }
    call.leading = headers
    call.options.onHeader?(headers)
    for waiter in call.headerWaiters { waiter.resume(returning: headers) }
    call.headerWaiters.removeAll()
  }
  private func finish(_ call: Call, error: Error? = nil, trailers: [String: String] = [:]) {
    guard !call.finished else { return }
    call.finished = true
    call.error = error
    call.sendClosed = true
    call.outbound.removeAll()
    call.outboundBytes = 0
    call.deadline?.cancel()
    call.deadline = nil
    call.pump?.cancel()
    call.pump = nil
    if calls[call.id] === call { calls.removeValue(forKey: call.id) }
    leading(call, [:])
    call.trailer = trailers
    call.options.onTrailer?(trailers)
    for waiter in call.trailerWaiters { waiter.resume(returning: trailers) }
    call.trailerWaiters.removeAll()
    for waiter in call.readers {
      if let error { waiter.resume(throwing: error) } else { waiter.resume(returning: nil) }
    }
    call.readers.removeAll()
  }
  private func consume(_ call: Call, _ data: Data) {
    guard !call.finished else { return }
    call.pendingCredit += data.count
    if call.pendingCredit >= max(1, initialWindow / 2) {
      frame(call, .windowUpdate, window: call.pendingCredit)
      call.pendingCredit = 0
    }
  }
  private func pull(_ call: Call) async throws -> Data? {
    if Task.isCancelled {
      abort(call, error: WsStatusError(code: 1, message: "cancelled"))
      throw WsStatusError(code: 1, message: "cancelled")
    }
    return try await withTaskCancellationHandler {
      if !call.inbound.isEmpty {
        let data = call.inbound.removeFirst()
        call.inboundBytes -= data.count
        consume(call, data)
        return data
      }
      if let error = call.error { throw error }
      if call.finished { return nil }
      return try await withCheckedThrowingContinuation { call.readers.append($0) }
    } onCancel: {
      Task { await self.abort(call, error: WsStatusError(code: 1, message: "cancelled")) }
    }
  }
  private func headers(_ call: Call) async -> [String: String] {
    await withTaskCancellationHandler {
      if Task.isCancelled { abort(call, error: WsStatusError(code: 1, message: "cancelled")) }
      if let value = call.leading { return value }
      return await withCheckedContinuation { call.headerWaiters.append($0) }
    } onCancel: {
      Task { await self.abort(call, error: WsStatusError(code: 1, message: "cancelled")) }
    }
  }
  private func trailers(_ call: Call) async -> [String: String] {
    await withTaskCancellationHandler {
      if Task.isCancelled { abort(call, error: WsStatusError(code: 1, message: "cancelled")) }
      if let value = call.trailer { return value }
      return await withCheckedContinuation { call.trailerWaiters.append($0) }
    } onCancel: {
      Task { await self.abort(call, error: WsStatusError(code: 1, message: "cancelled")) }
    }
  }
  private func handle(_ value: WireFrame) {
    func status() -> WsStatusError? {
      value.status.code == 0
        ? nil
        : WsStatusError(
          code: Int(value.status.code), message: value.status.message, details: value.status.details
        )
    }
    if value.kind == .connEnd, value.streamID == 0 {
      drop(
        status() ?? WsStatusError(code: 14, message: "connection ended by server"), reconnect: false
      )
      return
    }
    guard let call = calls[value.streamID] else { return }
    switch value.kind {
    case .header: leading(call, value.headers)
    case .windowUpdate:
      call.sendWindow += Int(value.window)
      pump(call)
    case .msg:
      if call.inboundBytes > initialWindow {
        abort(call, error: WsStatusError(code: 8, message: "receive buffer overflow"))
        return
      }
      leading(call, [:])
      if !call.readers.isEmpty {
        let waiter = call.readers.removeFirst()
        consume(call, value.payload)
        waiter.resume(returning: value.payload)
      } else {
        call.inbound.append(value.payload)
        call.inboundBytes += value.payload.count
      }
    case .end: finish(call, error: status(), trailers: value.headers)
    case .rst:
      finish(
        call, error: status() ?? WsStatusError(code: 1, message: "stream reset"),
        trailers: value.headers)
    default: break
    }
  }

  private func dispatch<Response: SwiftProtobuf.Message>(
    _ request: RPCRequest, responseType: Response.Type
  ) async throws -> RPCResponse {
    var handler: RPCHandler = { request in
      await self.terminalCall(request, responseType: responseType)
    }
    for interceptor in options.interceptors.reversed() {
      let next = handler
      handler = { try await interceptor.intercept($0, next: next) }
    }
    return try await handler(request)
  }
  private func terminalCall<Response: SwiftProtobuf.Message>(
    _ request: RPCRequest, responseType: Response.Type
  ) -> RPCResponse {
    let call = open(request.method, options: request.options)
    if !call.finished {
      call.pump = Task {
        do {
          guard !Task.isCancelled, !call.finished else { return }
          for try await message in request.messages {
            guard !Task.isCancelled, !call.finished else { return }
            self.send(call, payload: try message.serializedData())
          }
          self.closeSend(call)
        } catch { self.abort(call, error: error) }
      }
    }
    let messages = MessageSequence {
      MessageSequence.AsyncIterator {
        guard let bytes = try await self.pull(call) else { return nil }
        do { return try Response(serializedBytes: bytes) } catch {
          await self.abort(call, error: error)
          throw error
        }
      }
    }
    return RPCResponse(
      messages: messages, headers: { await self.headers(call) },
      trailers: { await self.trailers(call) }, cancel: { await self.abort(call) })
  }
  private func single<Response: SwiftProtobuf.Message>(
    _ response: RPCResponse, responseType: Response.Type, requireSingle: Bool
  ) async throws -> UnaryResponse<Response> {
    do {
      var result: Response?
      for try await message in response.messages {
        if requireSingle, result != nil {
          throw WsStatusError(code: 13, message: "server sent more than one response message")
        }
        guard let value = message as? Response else {
          throw WsStatusError(code: 13, message: "interceptor returned unexpected message type")
        }
        result = value
      }
      guard let result else {
        throw WsStatusError(code: 13, message: "server closed stream without a response")
      }
      return await UnaryResponse(
        message: result, headers: response.headers(), trailers: response.trailers())
    } catch {
      await response.cancel()
      throw error
    }
  }
  public func unary<Request: SwiftProtobuf.Message, Response: SwiftProtobuf.Message>(
    method: String, request: Request, responseType: Response.Type, options: CallOptions = .init()
  ) async throws -> UnaryResponse<Response> {
    let response = try await dispatch(
      RPCRequest(method: method, kind: .unary, options: options, messages: .one(request)),
      responseType: responseType)
    return try await single(response, responseType: responseType, requireSingle: true)
  }
  public func serverStreaming<Request: SwiftProtobuf.Message, Response: SwiftProtobuf.Message>(
    method: String, request: Request, responseType: Response.Type, options: CallOptions = .init()
  ) async throws -> ResponseStream<Response> {
    ResponseStream(
      try await dispatch(
        RPCRequest(
          method: method, kind: .serverStreaming, options: options, messages: .one(request)),
        responseType: responseType))
  }
  public func clientStreaming<
    Request: SwiftProtobuf.Message, Response: SwiftProtobuf.Message, Input: AsyncSequence & Sendable
  >(method: String, requests: Input, responseType: Response.Type, options: CallOptions = .init())
    async throws -> UnaryResponse<Response> where Input.Element == Request
  {
    let response = try await dispatch(
      RPCRequest(
        method: method, kind: .clientStreaming, options: options,
        messages: MessageSequence(requests)), responseType: responseType)
    return try await single(response, responseType: responseType, requireSingle: false)
  }
  public func bidiStreaming<
    Request: SwiftProtobuf.Message, Response: SwiftProtobuf.Message, Input: AsyncSequence & Sendable
  >(method: String, requests: Input, responseType: Response.Type, options: CallOptions = .init())
    async throws -> ResponseStream<Response> where Input.Element == Request
  {
    ResponseStream(
      try await dispatch(
        RPCRequest(
          method: method, kind: .bidiStreaming, options: options,
          messages: MessageSequence(requests)), responseType: responseType))
  }
}
