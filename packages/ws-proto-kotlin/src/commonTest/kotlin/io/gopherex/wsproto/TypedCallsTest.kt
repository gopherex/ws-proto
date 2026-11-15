package io.gopherex.wsproto

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import wsproto.transport.v1.Frame
import wsproto.transport.v1.Kind
import wsproto.transport.v1.Status
import kotlin.test.*

@OptIn(ExperimentalCoroutinesApi::class)
class TypedCallsTest {
    private val unary = MethodInfo<Status, Status>("test.Service", "Unary", MethodKind.UNARY, Status.ADAPTER, Status.ADAPTER)
    private val bidi = MethodInfo<Status, Status>("test.Service", "Bidi", MethodKind.BIDI_STREAMING, Status.ADAPTER, Status.ADAPTER)

    @Test fun `unary waits for END and propagates trailing errors`() = runTest {
        val socket = FakeSocketFactory()
        val transport = WsTransport("ws://localhost", socketFactory = socket, scope = backgroundScope)
        socket.open()
        val call = backgroundScope.async { runCatching { transport.unary(unary, Status()) } }
        testScheduler.runCurrent()
        val id = socket.frames.first().stream_id
        socket.inject(message(id))
        assertFalse(call.isCompleted)
        socket.inject(end(id, 7))
        assertEquals(7, assertIs<WsStatusException>(call.await().exceptionOrNull()).code)
        transport.close()
    }

    @Test fun `unary rejects extra response and resets live stream`() = runTest {
        val socket = FakeSocketFactory()
        val transport = WsTransport("ws://localhost", socketFactory = socket, scope = backgroundScope)
        socket.open()
        val call = backgroundScope.async { runCatching { transport.unary(unary, Status()) } }
        testScheduler.runCurrent()
        val id = socket.frames.first().stream_id
        socket.inject(message(id))
        socket.inject(message(id))
        assertEquals(13, assertIs<WsStatusException>(call.await().exceptionOrNull()).code)
        assertEquals(Kind.KIND_RST, socket.frames.last().kind)
        transport.close()
    }

    @Test fun `interceptors compose outermost first and can modify request metadata`() = runTest {
        val events = mutableListOf<String>()
        fun interceptor(name: String) = object : Interceptor {
            override suspend fun <I, O> unary(
                request: UnaryRequest<I, O>, next: suspend (UnaryRequest<I, O>) -> UnaryResponse<O>,
            ): UnaryResponse<O> {
                events += "$name before"
                val response = next(request.copy(options = request.options.copy(headers = request.options.headers + (name to "value"))))
                events += "$name after"
                return response
            }
        }
        val socket = FakeSocketFactory()
        val transport = WsTransport("ws://localhost", WsTransportOptions(interceptors = listOf(interceptor("one"), interceptor("two"))), socket, backgroundScope)
        socket.open()
        val call = backgroundScope.async { transport.unary(unary, Status()) }
        testScheduler.runCurrent()
        val open = socket.frames.first()
        assertEquals(mapOf("one" to "value", "two" to "value"), open.headers)
        socket.inject(message(open.stream_id))
        socket.inject(end(open.stream_id))
        call.await()
        assertEquals(listOf("one before", "two before", "two after", "one after"), events)
        transport.close()
    }

    @Test fun `idle request producer is cancelled when response ends`() = runTest {
        val socket = FakeSocketFactory()
        val transport = WsTransport("ws://localhost", socketFactory = socket, scope = backgroundScope)
        socket.open()
        var finalized = false
        val response = transport.stream(bidi, flow {
            try { emit(Status()); awaitCancellation() } finally { finalized = true }
        })
        testScheduler.runCurrent()
        socket.inject(end(socket.frames.first().stream_id))
        assertTrue(response.message.toList().isEmpty())
        testScheduler.runCurrent()
        assertTrue(finalized)
        assertEquals(emptyMap(), response.trailer.await())
        transport.close()
    }

    @Test fun `request source error surfaces and cancellation resets call`() = runTest {
        val socket = FakeSocketFactory()
        val transport = WsTransport("ws://localhost", socketFactory = socket, scope = backgroundScope)
        socket.open()
        val failure = IllegalStateException("source")
        val response = transport.stream(bidi, flow { throw failure })
        testScheduler.runCurrent()
        assertSame(failure, assertFailsWith<IllegalStateException> { response.message.toList() })
        assertEquals(Kind.KIND_RST, socket.frames.last().kind)
        val call = backgroundScope.async { transport.unary(unary, Status()) }
        testScheduler.runCurrent()
        call.cancel()
        testScheduler.runCurrent()
        assertFailsWith<CancellationException> { call.await() }
        assertEquals(Kind.KIND_RST, socket.frames.last().kind)
        transport.close()
    }

    @Test fun `producer cancellation terminates call instead of leaving it hanging`() = runTest {
        val socket = FakeSocketFactory()
        val transport = WsTransport("ws://localhost", socketFactory = socket, scope = backgroundScope)
        socket.open()
        val response = transport.stream(bidi, flow { throw CancellationException("producer stopped") })
        testScheduler.runCurrent()
        assertEquals(1, assertFailsWith<WsStatusException> { response.message.toList() }.code)
        assertEquals(Kind.KIND_RST, socket.frames.last().kind)
        transport.close()
    }

    @Test fun `read error wins over producer failure after server termination`() = runTest {
        val socket = FakeSocketFactory()
        val transport = WsTransport("ws://localhost", socketFactory = socket, scope = backgroundScope)
        socket.open()
        val response = transport.stream(bidi, flow {
            try { awaitCancellation() } finally { throw IllegalStateException("producer cleanup") }
        })
        testScheduler.runCurrent()
        socket.inject(end(socket.frames.first().stream_id, 7))
        testScheduler.runCurrent()
        assertEquals(7, assertFailsWith<WsStatusException> { response.message.toList() }.code)
        transport.close()
    }

    @Test fun `stream interceptor can short circuit without a wire call`() = runTest {
        val interceptor = object : Interceptor {
            override suspend fun <I, O> stream(
                request: StreamRequest<I, O>, next: suspend (StreamRequest<I, O>) -> StreamResponse<O>,
            ): StreamResponse<O> = StreamResponse(
                flowOf(request.method.output.decode(Status.ADAPTER.encode(Status(message = "cached")))),
                CompletableDeferred(mapOf("cache" to "hit")), CompletableDeferred(emptyMap()), {},
            )
        }
        val socket = FakeSocketFactory()
        val transport = WsTransport("ws://localhost", WsTransportOptions(interceptors = listOf(interceptor)), socket, backgroundScope)
        socket.open()
        val response = transport.stream(bidi, flowOf(Status()))
        assertEquals("cached", response.message.toList().single().message)
        assertEquals(mapOf("cache" to "hit"), response.header.await())
        assertTrue(socket.frames.isEmpty())
        transport.close()
    }

    private fun message(id: Int) = Frame(stream_id = id, kind = Kind.KIND_MSG, payload = Status.ADAPTER.encodeByteString(Status(message = "response")))
    private fun end(id: Int, code: Int = 0) = Frame(stream_id = id, kind = Kind.KIND_END, status = Status(code = code))
}
