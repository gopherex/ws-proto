import XCTest

@testable import WsProto

final class IntegrationTests: XCTestCase {
  func endpoint() throws -> URL {
    guard let value = ProcessInfo.processInfo.environment["WS_PROTO_TEST_URL"],
      let url = URL(string: value)
    else {
      throw XCTSkip(
        "Run scripts/with-mobile-server.sh swift test for the Go interoperability suite")
    }
    return url
  }
  func transport() throws -> WsTransport {
    let url = try endpoint()
    var options = TransportOptions()
    options.dialTimeoutMs = 5_000
    #if os(Linux)
      let transport = WsTransport(
        url: url, options: options, createSocket: { RelaySocket(url: $0) })
    #else
      let transport = WsTransport(url: url, options: options)
    #endif
    addTeardownBlock { await transport.close() }
    return transport
  }
  func testNativeURLSessionUnarySmokeAgainstGo() async throws {
    let transport = WsTransport(url: try endpoint())
    addTeardownBlock { await transport.close() }
    let response = try await transport.unary(
      method: "/echo.v1.EchoService/Unary",
      request: Echo_V1_UnaryRequest.with { $0.name = "native" },
      responseType: Echo_V1_UnaryResponse.self, options: CallOptions(timeoutMs: 5_000))
    XCTAssertEqual(response.message.greeting, "hello native")
  }
  func testAllRPCShapesAndMetadataAgainstGo() async throws {
    let transport = try transport()
    let client = Echo_V1_EchoServiceClient(transport: transport)
    let options = CallOptions(headers: ["x-test": "swift"], timeoutMs: 5_000)
    let unary = try await client.unary(
      Echo_V1_UnaryRequest.with { $0.name = "swift" }, options: options)
    XCTAssertEqual(unary.message.greeting, "hello swift")
    XCTAssertEqual(unary.headers["x-header"], "ready")
    XCTAssertEqual(unary.headers["x-echo"], "swift")
    XCTAssertEqual(unary.trailers["x-trailer"], "done")
    let server = try await client.serverStream(
      Echo_V1_ServerStreamRequest.with { $0.count = 3 }, options: options)
    var indices: [Int32] = []
    for try await message in server { indices.append(message.index) }
    XCTAssertEqual(indices, [0, 1, 2])
    let requests = AsyncStream<Echo_V1_ClientStreamRequest> { continuation in
      for number in 1...3 { continuation.yield(.with { $0.value = Int32(number) }) }
      continuation.finish()
    }
    let clientResponse = try await client.clientStream(requests, options: options)
    XCTAssertEqual(clientResponse.message.sum, 6)
    let inputs = AsyncStream<Echo_V1_BidiRequest> { continuation in
      continuation.yield(.with { $0.text = "one" })
      continuation.yield(.with { $0.text = "two" })
      continuation.finish()
    }
    let bidi = try await client.bidi(inputs, options: options)
    var echoes: [String] = []
    for try await value in bidi { echoes.append(value.echo) }
    XCTAssertEqual(echoes, ["echo:one", "echo:two"])
    await transport.close()
  }
  func testFlowControlLargeMessagesAndConcurrentRPCsAgainstGo() async throws {
    let transport = try transport()
    let text = String(repeating: "x", count: 400_000)
    try await withThrowingTaskGroup(of: Void.self) { group in
      for _ in 0..<4 {
        group.addTask {
          let source = AsyncStream<Echo_V1_BidiRequest> { continuation in
            for _ in 0..<4 { continuation.yield(.with { $0.text = text }) }
            continuation.finish()
          }
          let stream = try await transport.bidiStreaming(
            method: "/echo.v1.EchoService/Bidi", requests: source,
            responseType: Echo_V1_BidiResponse.self, options: CallOptions(timeoutMs: 10_000))
          var count = 0
          for try await message in stream {
            XCTAssertEqual(message.echo, "echo:" + text)
            count += 1
          }
          XCTAssertEqual(count, 4)
        }
      }
      try await group.waitForAll()
    }
    await transport.close()
  }

  func testFlowControlManyMessagesAndConcurrentRPCsAgainstGo() async throws {
    let transport = try transport()
    let text = String(repeating: "x", count: 1024)
    try await withThrowingTaskGroup(of: Void.self) { group in
      for _ in 0..<4 {
        group.addTask {
          let source = AsyncStream<Echo_V1_BidiRequest> { continuation in
            for _ in 0..<600 { continuation.yield(.with { $0.text = text }) }
            continuation.finish()
          }
          let stream = try await transport.bidiStreaming(
            method: "/echo.v1.EchoService/Bidi", requests: source,
            responseType: Echo_V1_BidiResponse.self, options: CallOptions(timeoutMs: 10_000))
          var count = 0
          for try await message in stream {
            XCTAssertEqual(message.echo, "echo:" + text)
            count += 1
          }
          XCTAssertEqual(count, 600)
        }
      }
      try await group.waitForAll()
    }
    await transport.close()
  }
  func testServerErrorDeadlineAndEarlyEndAgainstGo() async throws {
    let transport = try transport()
    do {
      _ = try await transport.unary(
        method: "/echo.v1.EchoService/Unary",
        request: Echo_V1_UnaryRequest.with { $0.name = "!error" },
        responseType: Echo_V1_UnaryResponse.self, options: CallOptions(timeoutMs: 5_000))
      XCTFail("expected server error")
    } catch let error as WsStatusError {
      XCTAssertEqual(error.code, 3)
      XCTAssertFalse(error.details.isEmpty)
    }
    do {
      _ = try await transport.unary(
        method: "/echo.v1.EchoService/Unary",
        request: Echo_V1_UnaryRequest.with { $0.name = "!wait" },
        responseType: Echo_V1_UnaryResponse.self, options: CallOptions(timeoutMs: 30))
      XCTFail("expected deadline")
    } catch let error as WsStatusError { XCTAssertEqual(error.code, 4) }
    let source = AsyncStream<Echo_V1_BidiRequest> { $0.yield(.with { $0.text = "!end" }) }
    let stream = try await transport.bidiStreaming(
      method: "/echo.v1.EchoService/Bidi", requests: source,
      responseType: Echo_V1_BidiResponse.self, options: CallOptions(timeoutMs: 5_000))
    for try await _ in stream { XCTFail("unexpected response") }
    await transport.close()
  }
}
