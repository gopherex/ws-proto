package io.gopherex.wsproto

import io.ktor.client.HttpClient
import io.ktor.client.engine.okhttp.OkHttp
import io.ktor.client.plugins.websocket.WebSockets

internal actual fun defaultHttpClient(): HttpClient = HttpClient(OkHttp) { install(WebSockets) }
internal actual fun currentTimeMillis(): Long = System.currentTimeMillis()
