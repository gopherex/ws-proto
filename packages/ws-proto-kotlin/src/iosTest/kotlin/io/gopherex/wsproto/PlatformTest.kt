package io.gopherex.wsproto

import kotlinx.cinterop.ExperimentalForeignApi
import kotlinx.cinterop.toKString
import platform.posix.getenv

@OptIn(ExperimentalForeignApi::class)
internal actual fun testServerUrl(): String = checkNotNull(getenv("WS_PROTO_TEST_URL")?.toKString()) {
    "Run integration tests with scripts/with-mobile-server.sh"
}
