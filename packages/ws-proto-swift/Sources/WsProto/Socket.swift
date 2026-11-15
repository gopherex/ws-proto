import Foundation

#if canImport(FoundationNetworking)
  import FoundationNetworking
#endif

public enum SocketEvent: Sendable {
  case opened(protocolName: String)
  case message(Data)
  case closed
}

/// A socket must deliver events in wire order and serialize each send as one binary message.
public protocol WsSocket: AnyObject, Sendable {
  var events: AsyncStream<SocketEvent> { get }
  func send(_ data: Data) async throws
  func close()
}

public final class URLSessionSocket: NSObject, WsSocket, URLSessionWebSocketDelegate, @unchecked
  Sendable
{
  public let events: AsyncStream<SocketEvent>
  private let continuation: AsyncStream<SocketEvent>.Continuation
  private var session: URLSession!
  private var socket: URLSessionWebSocketTask!
  private var reader: Task<Void, Never>?
  private let lock = NSLock()
  private var closed = false

  public init(url: URL, headers: [String: String] = [:]) {
    var continuation: AsyncStream<SocketEvent>.Continuation!
    events = AsyncStream { continuation = $0 }
    self.continuation = continuation
    super.init()
    let configuration = URLSessionConfiguration.default
    configuration.httpAdditionalHeaders = headers.filter {
      $0.key.lowercased() != "sec-websocket-protocol"
    }
    session = URLSession(configuration: configuration, delegate: self, delegateQueue: nil)
    socket = session.webSocketTask(with: url, protocols: ["wsrpc.v1"])
    // Enforce the configured cap in the mux so it retains the permanent
    // RESOURCE_EXHAUSTED cause rather than a generic native socket error.
    socket.maximumMessageSize = Int.max
    socket.resume()
  }

  public func urlSession(
    _ session: URLSession, webSocketTask: URLSessionWebSocketTask,
    didOpenWithProtocol protocol: String?
  ) {
    lock.lock()
    defer { lock.unlock() }
    guard !closed else { return }
    continuation.yield(.opened(protocolName: `protocol` ?? ""))
    let continuation = self.continuation
    reader = Task {
      do {
        while !Task.isCancelled {
          switch try await webSocketTask.receive() {
          case .data(let bytes): continuation.yield(.message(bytes))
          case .string: break
          @unknown default: break
          }
        }
      } catch {
        continuation.yield(.closed)
        continuation.finish()
      }
    }
  }
  public func urlSession(
    _ session: URLSession, task: URLSessionTask, didCompleteWithError error: Error?
  ) {
    continuation.yield(.closed)
    continuation.finish()
  }
  public func urlSession(
    _ session: URLSession, webSocketTask: URLSessionWebSocketTask,
    didCloseWith closeCode: URLSessionWebSocketTask.CloseCode, reason: Data?
  ) {
    continuation.yield(.closed)
    continuation.finish()
  }
  public func send(_ data: Data) async throws { try await socket.send(.data(data)) }
  public func close() {
    lock.lock()
    guard !closed else {
      lock.unlock()
      return
    }
    closed = true
    let reader = self.reader
    self.reader = nil
    lock.unlock()
    reader?.cancel()
    socket.cancel(with: .normalClosure, reason: nil)
    session.invalidateAndCancel()
    continuation.finish()
  }
}
