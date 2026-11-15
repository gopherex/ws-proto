import KmpFixture

// Kotlin/Objective-C export cannot express Swift Sendable. This fixture-owned
// final data class contains only immutable val Int/Int/String properties;
// Kotlin's memory manager permits these values to be read across threads.
// Revisit this conformance if WatchEvent gains mutable or reference state.
extension WatchEvent: @retroactive @unchecked Sendable {}
