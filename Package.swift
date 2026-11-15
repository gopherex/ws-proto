// swift-tools-version: 6.1
import PackageDescription

let package = Package(
  name: "WsProto",
  platforms: [.iOS(.v15), .macOS(.v12)],
  products: [.library(name: "WsProto", targets: ["WsProto"])],
  dependencies: [.package(url: "https://github.com/apple/swift-protobuf.git", from: "1.39.0")],
  targets: [
    .target(
      name: "WsProto", dependencies: [.product(name: "SwiftProtobuf", package: "swift-protobuf")],
      path: "packages/ws-proto-swift/Sources/WsProto"),
    .testTarget(
      name: "WsProtoTests", dependencies: ["WsProto"],
      path: "packages/ws-proto-swift/Tests/WsProtoTests", exclude: ["ws-relay.mjs"]),
  ],
  swiftLanguageModes: [.v6]
)
