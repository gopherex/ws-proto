import KmpFixture

/// Application code consumes the domain facade, not the protobuf transport.
public func makeEchoFacade(url: String) -> EchoFacade {
  EchoFacade(url: url)
}
