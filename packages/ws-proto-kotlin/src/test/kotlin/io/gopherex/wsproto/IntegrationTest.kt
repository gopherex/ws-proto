package io.gopherex.wsproto

import com.google.protobuf.Any
import com.google.protobuf.StringValue
import echo.v1.Echo.UnaryRequest
import echo.v1.Echo.UnaryResponse
import echo.v1.Echo.ServerStreamRequest
import echo.v1.Echo.ServerStreamResponse
import echo.v1.Echo.ClientStreamRequest
import echo.v1.Echo.ClientStreamResponse
import echo.v1.Echo.BidiRequest
import echo.v1.Echo.BidiResponse
import echo.v1.EchoServiceClient
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable
import kotlin.test.*

@EnabledIfEnvironmentVariable(named = "WS_PROTO_TEST_URL", matches = ".+")
class IntegrationTest {
    private val unary = MethodInfo<UnaryRequest, UnaryResponse>("echo.v1.EchoService", "Unary", MethodKind.UNARY, UnaryResponse.parser())
    private val server = MethodInfo<ServerStreamRequest, ServerStreamResponse>("echo.v1.EchoService", "ServerStream", MethodKind.SERVER_STREAMING, ServerStreamResponse.parser())
    private val client = MethodInfo<ClientStreamRequest, ClientStreamResponse>("echo.v1.EchoService", "ClientStream", MethodKind.CLIENT_STREAMING, ClientStreamResponse.parser())
    private val bidi = MethodInfo<BidiRequest, BidiResponse>("echo.v1.EchoService", "Bidi", MethodKind.BIDI_STREAMING, BidiResponse.parser())

    @Test fun `generated client executes all four shapes`() = runBlocking<Unit> {
        withTimeout(10_000) {
            WsTransport(System.getenv("WS_PROTO_TEST_URL")).use { transport ->
                val client = EchoServiceClient(transport)
                assertEquals("hello generated", client.unary(UnaryRequest.newBuilder().setName("generated").build()).greeting)
                assertEquals(3, client.serverStream(ServerStreamRequest.newBuilder().setCount(3).build()).toList().size)
                assertEquals(9, client.clientStream(flowOf(ClientStreamRequest.newBuilder().setValue(9).build())).sum)
                assertEquals("echo:typed", client.bidi(flowOf(BidiRequest.newBuilder().setText("typed").build())).toList().single().echo)
            }
        }
    }

    @Test fun `all four RPC shapes metadata large flow controlled input and concurrency`() = runBlocking<Unit> {
        withTimeout(30_000) {
            WsTransport(System.getenv("WS_PROTO_TEST_URL")).use { transport ->
                val result = transport.unary(unary, UnaryRequest.newBuilder().setName("Kotlin").build(), CallOptions(headers = mapOf("x-test" to "value")))
                assertEquals("hello Kotlin", result.message.greeting)
                assertEquals("ready", result.header["x-header"])
                assertEquals("value", result.header["x-echo"])
                assertEquals("done", result.trailer["x-trailer"])
                val responses = transport.stream(server, flowOf(ServerStreamRequest.newBuilder().setCount(1000).build()))
                assertEquals((0 until 1000).toList(), responses.message.map { it.index }.toList())
                val sum = firstResponse(transport.stream(client, flow {
                    repeat(100_000) { emit(ClientStreamRequest.newBuilder().setValue(1000).build()) }
                }))
                assertEquals(100_000_000, sum.sum)
                val echoes = transport.stream(bidi, flow {
                    repeat(10) { emit(BidiRequest.newBuilder().setText("x".repeat(300_000)).build()) }
                }).message.toList()
                assertEquals(10, echoes.size)
                assertTrue(echoes.all { it.echo == "echo:" + "x".repeat(300_000) })
                coroutineScope {
                    (0 until 30).map { n -> async {
                        assertEquals("hello $n", transport.unary(unary, UnaryRequest.newBuilder().setName("$n").build()).message.greeting)
                    } }.awaitAll()
                }
            }
        }
    }

    @Test fun `server errors details deadlines and absent response`() = runBlocking<Unit> {
        withTimeout(10_000) {
            WsTransport(System.getenv("WS_PROTO_TEST_URL")).use { transport ->
                var trailers: Map<String, String>? = null
                val error = assertFailsWith<WsStatusException> {
                    transport.unary(unary, UnaryRequest.newBuilder().setName("!error").build(), CallOptions(onTrailer = { trailers = it }))
                }
                assertEquals(3, error.code)
                assertEquals("mobile-error", error.message)
                val detail = Any.parseFrom(error.details.single())
                assertEquals("type.googleapis.com/google.protobuf.StringValue", detail.typeUrl)
                assertEquals("mobile-detail", StringValue.parseFrom(detail.value).value)
                assertEquals("done", trailers?.get("x-trailer"))
                assertEquals(4, assertFailsWith<WsStatusException> {
                    transport.unary(unary, UnaryRequest.newBuilder().setName("!wait").build(), CallOptions(timeoutMs = 50))
                }.code)
                assertEquals(13, assertFailsWith<WsStatusException> {
                    transport.unary(unary, UnaryRequest.newBuilder().setName("!empty").build())
                }.code)
            }
        }
    }

    @Test fun `early end and error never wait for idle infinite request source`() = runBlocking<Unit> {
        withTimeout(10_000) {
            for (command in listOf("!end", "!error")) {
                WsTransport(System.getenv("WS_PROTO_TEST_URL")).use { transport ->
                    val response = transport.stream(bidi, flow {
                        emit(BidiRequest.newBuilder().setText(command).build())
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

    @Test fun `breaking response consumption cancels server but keeps sibling calls working`() = runBlocking<Unit> {
        withTimeout(10_000) {
            WsTransport(System.getenv("WS_PROTO_TEST_URL")).use { transport ->
                val response = transport.stream(server, flowOf(ServerStreamRequest.newBuilder().setCount(100_000).build()))
                assertEquals(1, response.message.take(1).toList().size)
                assertEquals("hello alive", transport.unary(unary, UnaryRequest.newBuilder().setName("alive").build()).message.greeting)
            }
        }
    }
}
