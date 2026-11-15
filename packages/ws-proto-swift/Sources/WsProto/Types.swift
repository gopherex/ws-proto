import Foundation
import SwiftProtobuf

public struct WsStatusError: Error, Equatable, Sendable {
  public let code: Int
  public let message: String
  public let details: [Data]
  public init(code: Int, message: String, details: [Data] = []) {
    self.code = code
    self.message = message
    self.details = details
  }
}

public struct CallOptions: Sendable {
  public var headers: [String: String]
  public var timeoutMs: Int
  public var onHeader: (@Sendable ([String: String]) -> Void)?
  public var onTrailer: (@Sendable ([String: String]) -> Void)?
  public init(
    headers: [String: String] = [:], timeoutMs: Int = 0,
    onHeader: (@Sendable ([String: String]) -> Void)? = nil,
    onTrailer: (@Sendable ([String: String]) -> Void)? = nil
  ) {
    self.headers = headers
    self.timeoutMs = timeoutMs
    self.onHeader = onHeader
    self.onTrailer = onTrailer
  }
}

public enum ConnectionState: Equatable, Sendable {
  case paused, connecting, connected
  case waiting(retryAt: Date)
  case disconnected(WsStatusError)
  case closed
}

public struct TransportOptions: Sendable {
  public var initialWindow = 256 * 1024
  public var maxSendBuffer = 16 * 1024 * 1024
  public var maxFrameBytes = 0
  public var reconnect = false
  public var paused = false
  public var initialBackoffMs = 100
  public var maxBackoffMs = 30_000
  public var stableMs = 0
  public var dialTimeoutMs = 0
  public var headers: [String: String] = [:]
  public var interceptors: [any ClientInterceptor] = []
  public init() {}
}

public struct UnaryResponse<Message: SwiftProtobuf.Message>: Sendable {
  public var message: Message
  public var headers: [String: String]
  public var trailers: [String: String]
  public init(message: Message, headers: [String: String], trailers: [String: String]) {
    self.message = message
    self.headers = headers
    self.trailers = trailers
  }
}

/// Untyped RPC for applications that own their protobuf serialization.
public final class ClientStream: Sendable {
  private let write: @Sendable (Data) async -> Void
  private let halfClose: @Sendable () async -> Void
  private let read: @Sendable () async throws -> Data?
  private let leading: @Sendable () async -> [String: String]
  private let trailing: @Sendable () async -> [String: String]
  private let reset: @Sendable () async -> Void
  init(
    send: @escaping @Sendable (Data) async -> Void, closeSend: @escaping @Sendable () async -> Void,
    recv: @escaping @Sendable () async throws -> Data?,
    headers: @escaping @Sendable () async -> [String: String],
    trailers: @escaping @Sendable () async -> [String: String],
    cancel: @escaping @Sendable () async -> Void
  ) {
    write = send
    halfClose = closeSend
    read = recv
    leading = headers
    trailing = trailers
    reset = cancel
  }
  public func send(_ data: Data) async { await write(data) }
  public func closeSend() async { await halfClose() }
  public func recv() async throws -> Data? { try await read() }
  public func responseLeadingHeaders() async -> [String: String] { await leading() }
  public func responseHeaders() async -> [String: String] { await trailing() }
  public func cancel() async { await reset() }
}

/// Type-erased protobuf messages let transport-wide interceptors transform any RPC.
public struct MessageSequence: AsyncSequence, Sendable {
  public typealias Element = any SwiftProtobuf.Message
  public struct AsyncIterator: AsyncIteratorProtocol {
    private let pull: @Sendable () async throws -> Element?
    init(_ pull: @escaping @Sendable () async throws -> Element?) { self.pull = pull }
    public mutating func next() async throws -> Element? { try await pull() }
  }
  private let iterator: @Sendable () -> AsyncIterator
  public init(_ iterator: @escaping @Sendable () -> AsyncIterator) { self.iterator = iterator }
  public init<S: AsyncSequence & Sendable>(_ source: S) where S.Element: SwiftProtobuf.Message {
    self.iterator = {
      let box = IteratorBox(source.makeAsyncIterator())
      return AsyncIterator { try await box.next() }
    }
  }
  public static func one<M: SwiftProtobuf.Message>(_ message: M) -> MessageSequence {
    MessageSequence {
      let box = OnceMessage(message)
      return AsyncIterator { box.take() }
    }
  }
  public func makeAsyncIterator() -> AsyncIterator { iterator() }
  public func mapMessages(_ transform: @escaping @Sendable (Element) throws -> Element)
    -> MessageSequence
  {
    MessageSequence {
      let box = IteratorBox(self.makeAsyncIterator())
      return AsyncIterator {
        guard let message = try await box.next() else { return nil }
        return try transform(message)
      }
    }
  }
}

public enum MethodKind: Sendable { case unary, serverStreaming, clientStreaming, bidiStreaming }
public struct RPCRequest: Sendable {
  public var method: String
  public var kind: MethodKind
  public var options: CallOptions
  public var messages: MessageSequence
}
public struct RPCResponse: Sendable {
  public var messages: MessageSequence
  public var headers: @Sendable () async -> [String: String]
  public var trailers: @Sendable () async -> [String: String]
  public var cancel: @Sendable () async -> Void
  public init(
    messages: MessageSequence, headers: @escaping @Sendable () async -> [String: String],
    trailers: @escaping @Sendable () async -> [String: String],
    cancel: @escaping @Sendable () async -> Void
  ) {
    self.messages = messages
    self.headers = headers
    self.trailers = trailers
    self.cancel = cancel
  }
}
public typealias RPCHandler = @Sendable (RPCRequest) async throws -> RPCResponse
public protocol ClientInterceptor: Sendable {
  func intercept(_ request: RPCRequest, next: @escaping RPCHandler) async throws -> RPCResponse
}

public struct ResponseStream<Message: SwiftProtobuf.Message>: AsyncSequence, Sendable {
  public typealias Element = Message
  public final class AsyncIterator: AsyncIteratorProtocol {
    private var iterator: MessageSequence.AsyncIterator
    private let response: RPCResponse
    private var finished = false
    init(_ response: RPCResponse) {
      self.response = response
      iterator = response.messages.makeAsyncIterator()
    }
    public func next() async throws -> Message? {
      do {
        guard let message = try await iterator.next() else {
          finished = true
          return nil
        }
        guard let value = message as? Message else {
          throw WsStatusError(code: 13, message: "interceptor returned unexpected message type")
        }
        return value
      } catch {
        finished = true
        await response.cancel()
        throw error
      }
    }
    deinit {
      if !finished {
        let cancel = response.cancel
        Task { await cancel() }
      }
    }
  }
  private let response: RPCResponse
  init(_ response: RPCResponse) { self.response = response }
  public func makeAsyncIterator() -> AsyncIterator { AsyncIterator(response) }
  public func headers() async -> [String: String] { await response.headers() }
  public func trailers() async -> [String: String] { await response.trailers() }
  public func cancel() async { await response.cancel() }
}

// A lock transfers exclusive iterator ownership to each in-flight next().
// Concurrent pulls fail explicitly rather than racing on a caller's iterator.
private final class IteratorBox<I: AsyncIteratorProtocol>: @unchecked Sendable {
  private let lock = NSLock()
  private var iterator: I?
  init(_ iterator: I) { self.iterator = iterator }
  private func take() throws -> I {
    lock.lock()
    defer { lock.unlock() }
    guard let value = iterator else {
      throw WsStatusError(code: 9, message: "concurrent message iteration is not supported")
    }
    iterator = nil
    return value
  }
  private func restore(_ value: I) {
    lock.lock()
    defer { lock.unlock() }
    iterator = value
  }
  func next() async throws -> I.Element? {
    var value = try take()
    defer { restore(value) }
    return try await value.next()
  }
}
private final class OnceMessage: @unchecked Sendable {
  private let lock = NSLock()
  private var message: (any SwiftProtobuf.Message)?
  init(_ message: any SwiftProtobuf.Message) { self.message = message }
  func take() -> (any SwiftProtobuf.Message)? {
    lock.lock()
    defer { lock.unlock() }
    defer { message = nil }
    return message
  }
}
