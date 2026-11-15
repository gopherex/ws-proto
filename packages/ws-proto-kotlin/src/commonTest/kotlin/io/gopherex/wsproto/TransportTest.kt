package io.gopherex.wsproto

import okio.ByteString
import okio.ByteString.Companion.toByteString
import okio.ByteString.Companion.encodeUtf8
import kotlinx.coroutines.async
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import wsproto.transport.v1.Frame
import wsproto.transport.v1.Kind
import wsproto.transport.v1.Status
import kotlin.test.*

@OptIn(ExperimentalCoroutinesApi::class)
class TransportTest {
    @Test fun `flow control defers messages and half close until credit`() = runTest {
        val socket = FakeSocketFactory()
        val transport = WsTransport("ws://localhost/rpc", WsTransportOptions(initialWindow = 4), socket, backgroundScope)
        socket.open()
        val stream = transport.openStream("/test/Call")
        stream.send(ByteArray(8))
        stream.send(byteArrayOf(1))
        stream.closeSend()
        assertEquals(listOf(Kind.KIND_OPEN, Kind.KIND_MSG), socket.frames.map { it.kind })
        socket.inject(Frame(stream_id = stream.id, kind = Kind.KIND_WINDOW_UPDATE, window = 8))
        assertEquals(listOf(Kind.KIND_OPEN, Kind.KIND_MSG, Kind.KIND_MSG, Kind.KIND_HALF_CLOSE), socket.frames.map { it.kind })
        transport.close()
    }

    @Test fun `queued messages precede terminal error and metadata settles`() = runTest {
        val socket = FakeSocketFactory()
        val transport = WsTransport("ws://localhost/rpc", socketFactory = socket, scope = backgroundScope)
        socket.open()
        val stream = transport.openStream("/test/Call")
        socket.inject(Frame(stream_id = stream.id, kind = Kind.KIND_MSG, payload = "payload".encodeUtf8()))
        socket.inject(Frame(stream_id = stream.id, kind = Kind.KIND_END, headers = mapOf("trailer" to "value"), status = Status(code = 7, message = "denied")))
        assertEquals("payload", stream.recv()!!.decodeToString())
        assertEquals(7, assertFailsWith<WsStatusException> { stream.recv() }.code)
        assertEquals(emptyMap(), stream.header.await())
        assertEquals(mapOf("trailer" to "value"), stream.trailer.await())
        transport.close()
    }

    @Test fun `disconnect fails active and future calls without buffering`() = runTest {
        val socket = FakeSocketFactory()
        val transport = WsTransport("ws://localhost/rpc", socketFactory = socket, scope = backgroundScope)
        socket.open()
        val stream = transport.openStream("/test/Call")
        socket.drop()
        assertEquals(14, assertFailsWith<WsStatusException> { stream.recv() }.code)
        assertEquals(14, assertFailsWith<WsStatusException> { transport.openStream("/test/Next").recv() }.code)
        transport.close()
    }

    @Test fun `deadline is sent and locally terminates stream`() = runTest {
        val socket = FakeSocketFactory()
        val transport = WsTransport("ws://localhost/rpc", socketFactory = socket, scope = backgroundScope)
        socket.open()
        val stream = transport.openStream("/test/Call", CallOptions(timeoutMs = 10))
        assertEquals("10", socket.frames.first().headers["ws-timeout-ms"])
        testScheduler.advanceTimeBy(11)
        assertEquals(4, assertFailsWith<WsStatusException> { stream.recv() }.code)
        assertEquals(Kind.KIND_RST, socket.frames.last().kind)
        assertEquals(emptyMap(), stream.trailer.await())
        transport.close()
    }
}
