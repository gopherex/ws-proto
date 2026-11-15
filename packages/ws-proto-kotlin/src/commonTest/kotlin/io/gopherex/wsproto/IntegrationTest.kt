package io.gopherex.wsproto

import com.squareup.wire.AnyMessage
import com.squareup.wire.ProtoAdapter
import echo.v1.UnaryRequest
import echo.v1.UnaryResponse
import echo.v1.ServerStreamRequest
import echo.v1.ServerStreamResponse
import echo.v1.ClientStreamRequest
import echo.v1.ClientStreamResponse
import echo.v1.BidiRequest
import echo.v1.BidiResponse
import echo.v1.EchoServiceClient
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withTimeout
import kotlin.test.Test
import kotlin.test.*

class IntegrationTest {
    private val unary = MethodInfo<UnaryRequest, UnaryResponse>("echo.v1.EchoService", "Unary", MethodKind.UNARY, UnaryRequest.ADAPTER, UnaryResponse.ADAPTER)
    private val server = MethodInfo<ServerStreamRequest, ServerStreamResponse>("echo.v1.EchoService", "ServerStream", MethodKind.SERVER_STREAMING, ServerStreamRequest.ADAPTER, ServerStreamResponse.ADAPTER)
    private val client = MethodInfo<ClientStreamRequest, ClientStreamResponse>("echo.v1.EchoService", "ClientStream", MethodKind.CLIENT_STREAMING, ClientStreamRequest.ADAPTER, ClientStreamResponse.ADAPTER)
    private val bidi = MethodInfo<BidiRequest, BidiResponse>("echo.v1.EchoService", "Bidi", MethodKind.BIDI_STREAMING, BidiRequest.ADAPTER, BidiResponse.ADAPTER)

    @Test fun `generated client executes all four shapes`() = runTest {
        realTimeout(10_000) {
            WsTransport(testServerUrl()).use { transport ->
                val client = EchoServiceClient(transport)
                assertEquals("hello generated", client.unary(UnaryRequest(name = "generated")).greeting)
                assertEquals(3, client.serverStream(ServerStreamRequest(count = 3)).toList().size)
                assertEquals(9, client.clientStream(flowOf(ClientStreamRequest(value_ = 9))).sum)
                assertEquals("echo:typed", client.bidi(flowOf(BidiRequest(text = "typed"))).toList().single().echo)
            }
        }
    }

    @Test fun `all four RPC shapes metadata large flow controlled input and concurrency`() = runTest {
        realTimeout(30_000) {
            WsTransport(testServerUrl()).use { transport ->
                val result = transport.unary(unary, UnaryRequest(name = "Kotlin"), CallOptions(headers = mapOf("x-test" to "value")))
                assertEquals("hello Kotlin", result.message.greeting)
                assertEquals("ready", result.header["x-header"])
                assertEquals("value", result.header["x-echo"])
                assertEquals("done", result.trailer["x-trailer"])
                val responses = transport.stream(server, flowOf(ServerStreamRequest(count = 1000)))
                assertEquals((0 until 1000).toList(), responses.message.map { it.index }.toList())
                val sum = firstResponse(transport.stream(client, flow {
                    repeat(100_000) { emit(ClientStreamRequest(value_ = 1000)) }
                }))
                assertEquals(100_000_000, sum.sum)
                val echoes = transport.stream(bidi, flow {
                    repeat(10) { emit(BidiRequest(text = "x".repeat(300_000))) }
                }).message.toList()
                assertEquals(10, echoes.size)
                assertTrue(echoes.all { it.echo == "echo:" + "x".repeat(300_000) })
                coroutineScope {
                    (0 until 30).map { n -> async {
                        assertEquals("hello $n", transport.unary(unary, UnaryRequest(name = "$n")).message.greeting)
                    } }.awaitAll()
                }
            }
        }
    }

    @Test fun `server errors details deadlines and absent response`() = runTest {
        realTimeout(10_000) {
            WsTransport(testServerUrl()).use { transport ->
                var trailers: Map<String, String>? = null
                val error = assertFailsWith<WsStatusException> {
                    transport.unary(unary, UnaryRequest(name = "!error"), CallOptions(onTrailer = { trailers = it }))
                }
                assertEquals(3, error.code)
                assertEquals("mobile-error", error.message)
                val detail = AnyMessage.ADAPTER.decode(error.details.single())
                assertEquals("type.googleapis.com/google.protobuf.StringValue", detail.typeUrl)
                assertEquals("mobile-detail", ProtoAdapter.STRING_VALUE.decode(detail.value))
                assertEquals("done", trailers?.get("x-trailer"))
                assertEquals(4, assertFailsWith<WsStatusException> {
                    transport.unary(unary, UnaryRequest(name = "!wait"), CallOptions(timeoutMs = 50))
                }.code)
                assertEquals(13, assertFailsWith<WsStatusException> {
                    transport.unary(unary, UnaryRequest(name = "!empty"))
                }.code)
            }
        }
    }

    @Test fun `early end and error never wait for idle infinite request source`() = runTest {
        realTimeout(10_000) {
            for (command in listOf("!end", "!error")) {
                WsTransport(testServerUrl()).use { transport ->
                    val response = transport.stream(bidi, flow {
                        emit(BidiRequest(text = command))
                        awaitCancellation()
                    })
                    if (command == "!end") assertTrue(response.message.toList().isEmpty()) else {
                        assertEquals(3, assertFailsWith<WsStatusException> { response.message.toList() }.code)
                    }
                    assertEquals("done", response.trailer.await()["x-trailer"])
                }
            }
        }
    }

    @Test fun `breaking response consumption cancels server but keeps sibling calls working`() = runTest {
        realTimeout(10_000) {
            WsTransport(testServerUrl()).use { transport ->
                val response = transport.stream(server, flowOf(ServerStreamRequest(count = 100_000)))
                assertEquals(1, response.message.take(1).toList().size)
                assertEquals("hello alive", transport.unary(unary, UnaryRequest(name = "alive")).message.greeting)
            }
        }
    }
}

private suspend fun <T> realTimeout(timeoutMs: Long, block: suspend CoroutineScope.() -> T): T =
    withContext(Dispatchers.Default) { withTimeout(timeoutMs, block) }
