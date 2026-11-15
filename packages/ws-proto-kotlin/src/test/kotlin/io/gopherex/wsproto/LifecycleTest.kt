package io.gopherex.wsproto

import com.google.protobuf.ByteString
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.async
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test
import wsproto.transport.v1.Transport.Frame
import wsproto.transport.v1.Transport.Kind
import wsproto.transport.v1.Transport.Status
import kotlin.test.*
import java.util.concurrent.CyclicBarrier
import java.util.concurrent.Executors

@OptIn(ExperimentalCoroutinesApi::class)
class LifecycleTest {
    @Test fun `concurrent send and close never write after HALF_CLOSE`() = runTest {
        val executor = Executors.newFixedThreadPool(2)
        try {
            repeat(100) {
                val socket = FakeSocketFactory()
                val transport = WsTransport("ws://localhost", socketFactory = socket, scope = backgroundScope)
                socket.open()
                val stream = transport.openStream("/s/m")
                val barrier = CyclicBarrier(2)
                val send = executor.submit { barrier.await(); repeat(100) { stream.send(byteArrayOf(1)) } }
                val close = executor.submit { barrier.await(); stream.closeSend(); stream.closeSend() }
                send.get()
                close.get()
                assertEquals(1, socket.frames.count { it.kind == Kind.KIND_HALF_CLOSE })
                assertEquals(Kind.KIND_HALF_CLOSE, socket.frames.last().kind)
                stream.send(byteArrayOf(2))
                assertEquals(Kind.KIND_HALF_CLOSE, socket.frames.last().kind)
                transport.close()
            }
        } finally { executor.shutdownNow() }
    }

    @Test fun `credit returned on consumption only and not after terminal`() = runTest {
        val socket = FakeSocketFactory()
        val transport = WsTransport("ws://localhost", WsTransportOptions(initialWindow = 4), socket, backgroundScope)
        socket.open()
        val stream = transport.openStream("/s/m")
        socket.inject(message(stream.id, 3))
        assertEquals(1, socket.frames.size)
        assertEquals(3, stream.recv()!!.size)
        assertEquals(Kind.KIND_WINDOW_UPDATE, socket.frames.last().kind)
        assertEquals(3, socket.frames.last().window)
        socket.inject(message(stream.id, 3))
        socket.inject(Frame.newBuilder().setStreamId(stream.id).setKind(Kind.KIND_END).build())
        assertEquals(3, stream.recv()!!.size)
        assertEquals(2, socket.frames.size)
        transport.close()
    }

    @Test fun `oversized message admitted then malicious receive backlog reset`() = runTest {
        val socket = FakeSocketFactory()
        val transport = WsTransport("ws://localhost", WsTransportOptions(initialWindow = 4), socket, backgroundScope)
        socket.open()
        val stream = transport.openStream("/s/m")
        socket.inject(message(stream.id, 8))
        socket.inject(message(stream.id, 1))
        assertEquals(8, stream.recv()!!.size)
        assertEquals(8, assertFailsWith<WsStatusException> { stream.recv() }.code)
        assertEquals(Kind.KIND_RST, socket.frames.last().kind)
        transport.close()
    }

    @Test fun `outbound backlog bounded and released at terminal`() = runTest {
        val socket = FakeSocketFactory()
        val transport = WsTransport("ws://localhost", WsTransportOptions(initialWindow = 1, maxSendBuffer = 2), socket, backgroundScope)
        socket.open()
        val stream = transport.openStream("/s/m")
        stream.send(byteArrayOf(1))
        stream.send(ByteArray(3))
        stream.send(byteArrayOf(1))
        assertEquals(8, assertFailsWith<WsStatusException> { stream.recv() }.code)
        assertEquals(listOf(Kind.KIND_OPEN, Kind.KIND_MSG, Kind.KIND_RST), socket.frames.map { it.kind })
        transport.close()
    }

    @Test fun `preopen overflow is permanent and prevents reconnect`() = runTest {
        val socket = FakeSocketFactory()
        val transport = WsTransport("ws://localhost", WsTransportOptions(reconnect = true), socket, backgroundScope)
        val stream = transport.openStream("/s/m")
        stream.send(ByteArray(4 * 1024 * 1024))
        assertEquals(14, assertFailsWith<WsStatusException> { stream.recv() }.code)
        assertIs<ConnectionState.Disconnected>(transport.state.value)
        testScheduler.advanceTimeBy(60_000)
        assertEquals(1, socket.connects)
        assertTrue(socket.closed)
        transport.close()
    }

    @Test fun `wrong subprotocol and frame size fail permanently`() = runTest {
        for (wrongProtocol in listOf(true, false)) {
            val socket = FakeSocketFactory()
            val transport = WsTransport("ws://localhost", WsTransportOptions(reconnect = true, maxFrameBytes = 10), socket, backgroundScope)
            val stream = transport.openStream("/s/m")
            socket.open(if (wrongProtocol) "" else SUBPROTOCOL)
            if (!wrongProtocol) socket.inject(message(stream.id, 20))
            assertEquals(if (wrongProtocol) 14 else 8, assertFailsWith<WsStatusException> { stream.recv() }.code)
            assertIs<ConnectionState.Disconnected>(transport.state.value)
            testScheduler.advanceTimeBy(60_000)
            assertEquals(1, socket.connects)
            transport.close()
        }
    }

    @Test fun `connection end retains details and prevents redial and later calls`() = runTest {
        val socket = FakeSocketFactory()
        val transport = WsTransport("ws://localhost", WsTransportOptions(reconnect = true), socket, backgroundScope)
        socket.open()
        val stream = transport.openStream("/s/m")
        socket.inject(Frame.newBuilder().setKind(Kind.KIND_CONN_END).setStatus(Status.newBuilder().setCode(7).setMessage("expired").addDetails(ByteString.copyFromUtf8("detail"))).build())
        val error = assertFailsWith<WsStatusException> { stream.recv() }
        assertEquals(7, error.code)
        assertEquals("detail", error.details.single().toStringUtf8())
        assertSame(error, assertFailsWith<WsStatusException> { transport.openStream("/s/next").recv() })
        testScheduler.advanceTimeBy(60_000)
        assertEquals(1, socket.connects)
        transport.close()
    }

    @Test fun `pause preserves open socket and resumes after connection loss`() = runTest {
        val socket = FakeSocketFactory()
        val transport = WsTransport("ws://localhost", WsTransportOptions(reconnect = true), socket, backgroundScope)
        socket.open()
        transport.pause()
        assertEquals(ConnectionState.Connected, transport.state.value)
        assertFalse(socket.closed)
        socket.drop()
        assertEquals(ConnectionState.Paused, transport.state.value)
        transport.retryNow()
        assertEquals(1, socket.connects)
        transport.resume()
        assertEquals(2, socket.connects)
        transport.close()
    }

    @Test fun `pause aborts connecting calls and retryNow replaces waiting connection`() = runTest {
        val socket = FakeSocketFactory()
        val transport = WsTransport("ws://localhost", WsTransportOptions(reconnect = true), socket, backgroundScope)
        val buffered = transport.openStream("/s/m")
        transport.pause()
        assertEquals(14, assertFailsWith<WsStatusException> { buffered.recv() }.code)
        assertEquals(ConnectionState.Paused, transport.state.value)
        transport.resume()
        socket.open()
        socket.drop()
        assertIs<ConnectionState.Waiting>(transport.state.value)
        assertEquals(14, assertFailsWith<WsStatusException> { transport.openStream("/s/gap").recv() }.code)
        transport.retryNow()
        assertEquals(3, socket.connects)
        transport.close()
        testScheduler.advanceTimeBy(60_000)
        assertEquals(3, socket.connects)
    }

    @Test fun `dial timeout fails buffered calls and schedules fresh attempt`() = runTest {
        val socket = FakeSocketFactory()
        val transport = WsTransport("ws://localhost", WsTransportOptions(dialTimeoutMs = 10, reconnect = true), socket, backgroundScope)
        val stream = transport.openStream("/s/m")
        testScheduler.advanceTimeBy(11)
        assertEquals("dial timeout", assertFailsWith<WsStatusException> { stream.recv() }.message)
        transport.close()
    }

    @Test fun `early response collector cancellation resets only its stream`() = runTest {
        val socket = FakeSocketFactory()
        val transport = WsTransport("ws://localhost", socketFactory = socket, scope = backgroundScope)
        socket.open()
        val stream = transport.openStream("/s/m")
        val sibling = transport.openStream("/s/other")
        socket.inject(message(stream.id, 1))
        assertEquals(1, stream.messages.take(1).toList().size)
        assertEquals(Kind.KIND_RST, socket.frames.last().kind)
        socket.inject(Frame.newBuilder().setStreamId(sibling.id).setKind(Kind.KIND_END).build())
        assertNull(sibling.recv())
        transport.close()
    }

    private fun message(id: Int, size: Int) = Frame.newBuilder().setStreamId(id).setKind(Kind.KIND_MSG)
        .setPayload(ByteString.copyFrom(ByteArray(size))).build()
}
