import Foundation
import KmpConsumer
import KmpFixture
import XCTest

final class InteropTests: XCTestCase {
  private struct Probe: Decodable {
    let started: Bool
    let finished: Bool
    let cancelled: Bool
  }
  private enum ProbeFailure: Error { case timeout }

  private func facade() -> EchoFacade {
    let facade = makeEchoFacade(url: fixtureEndpoint)
    addTeardownBlock { facade.close() }
    return facade
  }

  private func waitForProbe(_ id: String, until predicate: (Probe) -> Bool) async throws {
    var url = URLComponents(string: fixtureEndpoint)!
    url.scheme = "http"
    url.path = "/probe"
    url.queryItems = [URLQueryItem(name: "id", value: id)]
    let deadline = Date().addingTimeInterval(5)
    var last: Probe?
    while Date() < deadline {
      var request = URLRequest(url: url.url!)
      request.timeoutInterval = 2
      let (data, response) = try await URLSession.shared.data(for: request)
      XCTAssertEqual((response as? HTTPURLResponse)?.statusCode, 200)
      let probe = try JSONDecoder().decode(Probe.self, from: data)
      if predicate(probe) { return }
      last = probe
      try await Task.sleep(nanoseconds: 20_000_000)
    }
    XCTFail("server probe \(id) did not reach the expected state; last=\(String(describing: last))")
    throw ProbeFailure.timeout
  }

  @MainActor
  func testSuspendFunctionRunsRealRPC() async throws {
    let facade = facade()
    let greeting = try await facade.greet(name: "SKIE")
    XCTAssertEqual(greeting, "hello SKIE")
  }

  @MainActor
  func testFlowIsTypedSwiftAsyncSequence() async {
    let facade = facade()
    var indexes: [Int] = []
    for await event in facade.watch(count: 5, probeId: UUID().uuidString) {
      XCTAssertEqual(event.errorCode, 0)
      indexes.append(Int(event.index))
    }
    XCTAssertEqual(indexes, [0, 1, 2, 3, 4])
  }

  @MainActor
  func testFlowFailureArrivesAsDomainData() async {
    let facade = facade()
    var errors: [String] = []
    for await event in facade.rejectedWatch() {
      XCTAssertEqual(event.errorCode, 3)
      errors.append(event.errorMessage)
    }
    XCTAssertEqual(errors, ["mobile-error"])
  }

  @MainActor
  func testSwiftTaskCancellationReachesSuspendingRPCOnServer() async throws {
    let facade = facade()
    let id = UUID().uuidString
    let completed = expectation(description: "SKIE suspend call returns after task cancellation")
    let task = Task {
      defer { completed.fulfill() }
      do {
        _ = try await facade.waitForCancellation(probeId: id)
        XCTFail("cancelled waiting RPC unexpectedly succeeded")
      } catch {
        XCTAssertTrue(Task.isCancelled)
      }
    }
    defer { task.cancel() }
    try await waitForProbe(id) { $0.started && !$0.finished }
    task.cancel()
    await fulfillment(of: [completed], timeout: 5)
    try await waitForProbe(id) { $0.finished && $0.cancelled }
  }

  @MainActor
  func testSwiftTaskCancellationReachesFlowRPCOnServer() async throws {
    let facade = facade()
    let id = UUID().uuidString
    let completed = expectation(description: "SKIE AsyncSequence ends after task cancellation")
    let task = Task {
      defer { completed.fulfill() }
      for await _ in facade.watch(count: -1, probeId: id) {
        XCTFail("waiting stream unexpectedly produced a value")
      }
    }
    defer { task.cancel() }
    try await waitForProbe(id) { $0.started && !$0.finished }
    task.cancel()
    await fulfillment(of: [completed], timeout: 5)
    try await waitForProbe(id) { $0.finished && $0.cancelled }
  }
}
