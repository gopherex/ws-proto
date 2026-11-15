package io.gopherex.wsproto

import com.google.protobuf.ByteString
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import wsproto.transport.v1.Transport.Frame
import wsproto.transport.v1.Transport.Kind

/** A raw RPC. send/closeSend are ordered, thread-safe, and never block a socket reader. */
class ClientStream internal constructor(
    val id: Int,
    private val lock: Any,
    private val initialWindow: Int,
    private val maxSendBuffer: Int,
    private val write: (Frame) -> Unit,
    private val detach: () -> Unit,
) {
    private val inbound = Channel<ByteArray>(Channel.UNLIMITED)
    private val outbound = ArrayDeque<ByteArray>()
    private var receivedBytes = 0L
    private var outboundBytes = 0L
    private var credit = initialWindow.toLong()
    private var pendingCredit = 0L
    private var sendClosed = false
    private var halfClosed = false
    internal var finished = false
        private set
    private val completion = mutableListOf<() -> Unit>()
    private val leading = CompletableDeferred<Map<String, String>>()
    private val trailing = CompletableDeferred<Map<String, String>>()
    val header: Deferred<Map<String, String>> get() = leading
    val trailer: Deferred<Map<String, String>> get() = trailing

    fun send(payload: ByteArray) = synchronized(lock) {
        if (finished || sendClosed) return@synchronized
        if (outboundBytes > maxSendBuffer) {
            abort(WsStatusException(8, "send buffer overflow"))
            return@synchronized
        }
        outbound.addLast(payload.copyOf())
        outboundBytes += payload.size
        pump()
    }

    fun closeSend() = synchronized(lock) {
        if (finished || sendClosed) return@synchronized
        sendClosed = true
        pump()
    }

    private fun pump() {
        while (!finished && credit > 0 && outbound.isNotEmpty()) {
            val bytes = outbound.removeFirst()
            outboundBytes -= bytes.size
            credit -= bytes.size
            write(frame(Kind.KIND_MSG).setPayload(ByteString.copyFrom(bytes)).build())
        }
        // HALF_CLOSE cannot overtake requests waiting for peer credit.
        if (!finished && sendClosed && outbound.isEmpty() && !halfClosed) {
            halfClosed = true
            write(frame(Kind.KIND_HALF_CLOSE).build())
        }
    }

    suspend fun recv(): ByteArray? {
        val result = try {
            inbound.receiveCatching()
        } catch (error: CancellationException) {
            abort(WsStatusException(1, "call cancelled", cause = error))
            throw error
        }
        result.exceptionOrNull()?.let { throw it }
        val bytes = result.getOrNull() ?: return null
        synchronized(lock) {
            receivedBytes -= bytes.size
            if (!finished) {
                pendingCredit += bytes.size
                if (pendingCredit >= maxOf(1, initialWindow / 2)) {
                    val delta = pendingCredit
                    pendingCredit = 0
                    write(frame(Kind.KIND_WINDOW_UPDATE).setWindow(delta.toInt()).build())
                }
            }
        }
        return bytes
    }

    val messages: Flow<ByteArray> get() = flow {
        try {
            while (true) emit(recv() ?: break)
        } finally {
            cancel()
        }
    }

    /** Explicit cancellation is a clean end, matching the TS raw stream cancel(). */
    fun cancel() = synchronized(lock) {
        if (!finished) {
            write(frame(Kind.KIND_RST).build())
            finish(null)
        }
    }

    internal fun abort(error: WsStatusException) = synchronized(lock) {
        if (!finished) {
            write(frame(Kind.KIND_RST).build())
            finish(error)
        }
    }

    internal fun onClose(callback: () -> Unit) = synchronized(lock) {
        if (finished) callback() else completion.add(callback)
    }

    internal fun credit(delta: Int) {
        val unsigned = delta.toLong() and 0xffffffffL
        if (unsigned == 0L || finished) return
        credit += unsigned
        pump()
    }

    internal fun push(bytes: ByteArray) {
        if (finished) return
        if (receivedBytes > initialWindow) {
            abort(WsStatusException(8, "receive buffer overflow"))
            return
        }
        leading.complete(emptyMap())
        receivedBytes += bytes.size
        inbound.trySend(bytes)
    }

    internal fun leading(headers: Map<String, String>) { leading.complete(headers.toMap()) }

    internal fun finish(error: Throwable?, headers: Map<String, String> = emptyMap()) {
        if (finished) return
        finished = true
        sendClosed = true
        outbound.clear()
        outboundBytes = 0
        detach()
        leading.complete(emptyMap())
        trailing.complete(headers.toMap())
        // close preserves queued messages, then exposes the terminal error.
        inbound.close(error)
        val callbacks = completion.toList()
        completion.clear()
        callbacks.forEach { it() }
    }

    private fun frame(kind: Kind) = Frame.newBuilder().setStreamId(id).setKind(kind)
}
