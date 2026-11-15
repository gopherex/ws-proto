package io.gopherex.wsproto

internal actual fun testServerUrl(): String = checkNotNull(System.getenv("WS_PROTO_TEST_URL")) {
    "Run integration tests with scripts/with-mobile-server.sh"
}
