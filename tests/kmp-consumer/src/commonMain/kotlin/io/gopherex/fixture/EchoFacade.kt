package io.gopherex.fixture

import echo.v1.BidiRequest
import echo.v1.EchoServiceClient
import echo.v1.ServerStreamRequest
import echo.v1.UnaryRequest
import io.gopherex.wsproto.CallOptions
import io.gopherex.wsproto.WsStatusException
import io.gopherex.wsproto.WsTransport
import io.gopherex.wsproto.WsTransportOptions
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.catch
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.map

/** A domain-facing API: Swift does not need protobuf, Ktor, or RPC transport types. */
class EchoFacade(url: String) {
    private val transport = WsTransport(url, WsTransportOptions(dialTimeoutMs = 5_000))
    private val client = EchoServiceClient(transport)

    @Throws(Exception::class)
    suspend fun greet(name: String): String =
        client.unary(UnaryRequest(name = name), CallOptions(timeoutMs = 5_000)).greeting

    /** No deadline: the simulator test proves Swift Task cancellation reaches the server. */
    @Throws(Exception::class)
    suspend fun waitForCancellation(probeId: String): String =
        client.unary(
            UnaryRequest(name = "!wait"),
            CallOptions(headers = mapOf("x-probe-id" to probeId)),
        ).greeting

    fun watch(count: Int, probeId: String): Flow<WatchEvent> =
        client.serverStream(
            ServerStreamRequest(count = count),
            CallOptions(
                headers = mapOf("x-probe-id" to probeId),
                timeoutMs = if (count < 0) 0 else 5_000,
            ),
        ).map { WatchEvent(index = it.index) }.asDomainEvents()

    fun rejectedWatch(): Flow<WatchEvent> =
        client.bidi(flowOf(BidiRequest(text = "!error")), CallOptions(timeoutMs = 5_000))
            .map { WatchEvent(index = 0) }.asDomainEvents()

    fun close() = transport.close()

    private fun Flow<WatchEvent>.asDomainEvents(): Flow<WatchEvent> = catch { error ->
        // SKIE Flow cannot throw arbitrary Kotlin exceptions into Swift.
        // Domain failures travel as values; cancellation remains structural.
        if (error is CancellationException) throw error
        val status = error as? WsStatusException
        emit(WatchEvent(index = -1, errorCode = status?.code ?: 13, errorMessage = error.message.orEmpty()))
    }
}

data class WatchEvent(val index: Int, val errorCode: Int = 0, val errorMessage: String = "")
