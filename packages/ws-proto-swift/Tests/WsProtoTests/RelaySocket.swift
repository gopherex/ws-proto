#if os(Linux)
  import Foundation
  import XCTest
  @testable import WsProto

  /// Test-only framing adapter for Linux FoundationNetworking's chunking defect.
  /// Apple tests exercise the shipping URLSessionSocket instead.
  final class RelaySocket: WsSocket, @unchecked Sendable {
    let events: AsyncStream<SocketEvent>
    private let continuation: AsyncStream<SocketEvent>.Continuation
    private let process = Process()
    private let input = Pipe()
    private let output = Pipe()
    private let lock = NSLock()
    private var closed = false
    private var started = false

    init(url: URL) {
      var continuation: AsyncStream<SocketEvent>.Continuation!
      events = AsyncStream { continuation = $0 }
      self.continuation = continuation
      let script = URL(fileURLWithPath: #filePath).deletingLastPathComponent()
        .appendingPathComponent("ws-relay.mjs")
      process.executableURL = URL(fileURLWithPath: "/usr/bin/env")
      process.arguments = ["node", script.path, url.absoluteString]
      // The Ubuntu Swift toolchain needs a private libcurl on Fedora. It must
      // not replace the system libraries in the independent Node process.
      var environment = ProcessInfo.processInfo.environment
      environment.removeValue(forKey: "LD_LIBRARY_PATH")
      process.environment = environment
      process.standardInput = input
      process.standardOutput = output
      do {
        try process.run()
        started = true
      } catch {
        XCTFail("could not start test relay: \(error)")
        continuation.yield(.closed)
        continuation.finish()
        return
      }
      let handle = output.fileHandleForReading
      let events = self.continuation
      DispatchQueue.global().async {
        var buffer = Data()
        while true {
          let data = handle.availableData
          if data.isEmpty { break }
          buffer.append(data)
          while let end = buffer.firstIndex(of: 10) {
            let line = Data(buffer[..<end])
            buffer.removeSubrange(...end)
            guard let value = try? JSONSerialization.jsonObject(with: line) as? [String: Any] else {
              continue
            }
            if let selected = value["open"] as? String {
              events.yield(.opened(protocolName: selected))
            } else if let body = value["data"] as? String, let bytes = Data(base64Encoded: body) {
              events.yield(.message(bytes))
            } else if value["closed"] as? Bool == true {
              events.yield(.closed)
            }
          }
        }
        events.yield(.closed)
        events.finish()
      }
    }
    func send(_ data: Data) async throws { try write(data) }
    private func write(_ data: Data) throws {
      lock.lock()
      defer { lock.unlock() }
      guard !closed else { throw WsStatusError(code: 14, message: "relay closed") }
      try input.fileHandleForWriting.write(
        contentsOf: Data((data.base64EncodedString() + "\n").utf8))
    }
    func close() {
      lock.lock()
      guard !closed else {
        lock.unlock()
        return
      }
      closed = true
      try? input.fileHandleForWriting.close()
      if process.isRunning { process.terminate() }
      lock.unlock()
      if started { process.waitUntilExit() }
      continuation.finish()
    }
    deinit { close() }
  }
#endif
