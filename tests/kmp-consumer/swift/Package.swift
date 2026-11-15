// swift-tools-version: 6.1
import PackageDescription

let package = Package(
  name: "KmpConsumer",
  platforms: [.iOS(.v15)],
  products: [.library(name: "KmpConsumer", targets: ["KmpConsumer"])],
  targets: [
    .binaryTarget(name: "KmpFixture", path: "KmpFixture.xcframework"),
    .target(name: "KmpConsumer", dependencies: ["KmpFixture"]),
    .testTarget(name: "KmpConsumerTests", dependencies: ["KmpConsumer", "KmpFixture"]),
  ],
  swiftLanguageModes: [.v6]
)
