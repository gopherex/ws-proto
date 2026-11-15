package io.gopherex.wsproto

import io.ktor.client.HttpClient
import io.ktor.client.engine.darwin.Darwin
import io.ktor.client.plugins.websocket.WebSockets
import platform.Foundation.NSDate
import platform.Foundation.timeIntervalSince1970

internal actual fun defaultHttpClient(): HttpClient = HttpClient(Darwin) { install(WebSockets) }
internal actual fun currentTimeMillis(): Long = (NSDate().timeIntervalSince1970 * 1000).toLong()
