package io.gopherex.wsproto

import com.google.protobuf.MessageLite
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.job
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import wsproto.transport.v1.Transport.Frame
import wsproto.transport.v1.Transport.Kind
import wsproto.transport.v1.Transport.Status
import java.io.Closeable
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.random.Random

/** Multiplexed client transport. Reconnect creates a new connection; it never replays RPCs. */
class WsTransport(
    private val url: String,
    private val options: WsTransportOptions = WsTransportOptions(),
    private val socketFactory: WsSocketFactory = OkHttpSocketFactory(),
    scope: CoroutineScope = CoroutineScope(Dispatchers.Default),
) : Closeable {
    private val lock = Any()
    private val owner = SupervisorJob(scope.coroutineContext[Job])
    private val scope = CoroutineScope(scope.coroutineContext + owner)
    private val mutableState = MutableStateFlow<ConnectionState>(ConnectionState.Connecting)
    val state: StateFlow<ConnectionState> = mutableState.asStateFlow()
    private var connection: Connection? = null
    private var paused = options.paused
    private var closed = false
    private var attempt = 0
    private var retry: Job? = null
    private var dialTimer: Job? = null
    private var stableTimer: Job? = null
    private val initialWindow = options.initialWindow.takeIf { it > 0 } ?: 256 * 1024
    private val maxSendBuffer = options.maxSendBuffer.takeIf { it > 0 } ?: 16 * 1024 * 1024

    init {
        synchronized(lock) {
            if (paused) mutableState.value = ConnectionState.Paused else dial()
        }
        owner.invokeOnCompletion { close() }
    }

    fun pause() = synchronized(lock) {
        if (closed || paused) return@synchronized
        paused = true
        retry?.cancel()
        retry = null
        when (mutableState.value) {
            is ConnectionState.Waiting -> mutableState.value = ConnectionState.Paused
            ConnectionState.Connecting -> {
                dialTimer?.cancel()
                connection?.terminate(WsStatusException(14, "dial paused"), false, notify = false)
                mutableState.value = ConnectionState.Paused
            }
            else -> Unit
        }
    }

    fun resume() = synchronized(lock) {
        if (closed || !paused) return@synchronized
        paused = false
        attempt = 0
        if (mutableState.value == ConnectionState.Paused) dial()
    }

    fun retryNow() = synchronized(lock) {
        if (closed || paused) return@synchronized
        when (mutableState.value) {
            is ConnectionState.Waiting, is ConnectionState.Disconnected -> dial()
            else -> Unit
        }
    }

    private fun dial() {
        retry?.cancel()
        retry = null
        val current = Connection()
        connection = current
        mutableState.value = ConnectionState.Connecting
        try {
            current.socket = socketFactory.connect(url, options.headers, current)
            if (current.terminal != null) current.socket?.close()
        } catch (error: Exception) {
            current.terminate(WsStatusException(14, "connection lost", cause = error), true)
        }
        if (current.terminal == null && !current.open && options.dialTimeoutMs > 0) {
            dialTimer = scope.launch {
                delay(options.dialTimeoutMs)
                synchronized(lock) {
                    if (connection === current && !current.open) {
                        current.terminate(WsStatusException(14, "dial timeout"), true)
                    }
                }
            }
        }
    }

    private fun dropped(error: WsStatusException, redial: Boolean) {
        dialTimer?.cancel()
        stableTimer?.cancel()
        if (closed) return
        if (!redial || !options.reconnect) {
            mutableState.value = ConnectionState.Disconnected(error)
        } else if (paused) {
            mutableState.value = ConnectionState.Paused
        } else {
            val maximum = options.backoff.maxMs.takeIf { it > 0 } ?: 30_000
            var limit = (options.backoff.initialMs.takeIf { it > 0 } ?: 100).coerceAtMost(maximum)
            repeat(attempt.coerceAtMost(62)) { limit = if (limit > maximum / 2) maximum else limit * 2 }
            attempt = (attempt + 1).coerceAtMost(62)
            val wait = Random.nextLong(limit.coerceAtMost(Long.MAX_VALUE - 1) + 1)
            mutableState.value = ConnectionState.Waiting(System.currentTimeMillis() + wait)
            val failedConnection = connection
            retry = scope.launch {
                delay(wait)
                synchronized(lock) {
                    // A timer already past delay may wait on this lock while retryNow replaces it.
                    if (!closed && !paused && connection === failedConnection && mutableState.value is ConnectionState.Waiting) dial()
                }
            }
        }
    }

    fun openStream(method: String, options: CallOptions = CallOptions()): ClientStream = synchronized(lock) {
        val current = connection
        if (closed || current == null) {
            return@synchronized ClientStream(1, lock, initialWindow, maxSendBuffer, {}, {}).apply {
                finish(if (closed) WsStatusException(1, "transport closed") else WsStatusException(14, "not connected"))
            }
        }
        current.openStream(method, options)
    }

    suspend fun <I : MessageLite, O : MessageLite> unary(
        method: MethodInfo<I, O>,
        message: I,
        options: CallOptions = CallOptions(),
    ): UnaryResponse<O> {
        var next: suspend (UnaryRequest<I, O>) -> UnaryResponse<O> = { unaryTerminal(it) }
        this.options.interceptors.asReversed().forEach { interceptor ->
            val inner = next
            next = { interceptor.unary(it, inner) }
        }
        return next(UnaryRequest(method, message, options))
    }

    private suspend fun <I : MessageLite, O : MessageLite> unaryTerminal(request: UnaryRequest<I, O>): UnaryResponse<O> {
        val stream = openStream(request.method.route, request.options)
        try {
            stream.send(request.message.toByteArray())
            stream.closeSend()
            val bytes = stream.recv() ?: throw WsStatusException(13, "server closed stream without a response")
            request.options.onHeader?.invoke(stream.header.await())
            val result = request.method.output.parseFrom(bytes)
            if (stream.recv() != null) throw WsStatusException(13, "server sent more than one response message")
            return UnaryResponse(result, stream.header.await(), stream.trailer.await())
        } finally {
            stream.cancel()
            request.options.onTrailer?.invoke(stream.trailer.await())
        }
    }

    suspend fun <I : MessageLite, O : MessageLite> stream(
        method: MethodInfo<I, O>,
        input: Flow<I>,
        options: CallOptions = CallOptions(),
    ): StreamResponse<O> {
        var next: suspend (StreamRequest<I, O>) -> StreamResponse<O> = { streamTerminal(it) }
        this.options.interceptors.asReversed().forEach { interceptor ->
            val inner = next
            next = { interceptor.stream(it, inner) }
        }
        return next(StreamRequest(method, input, options))
    }

    private suspend fun <I : MessageLite, O : MessageLite> streamTerminal(request: StreamRequest<I, O>): StreamResponse<O> {
        val stream = openStream(request.method.route, request.options)
        var pumpError: Throwable? = null
        val pump = scope.launch(start = CoroutineStart.LAZY) {
            try {
                request.message.collect { stream.send(it.toByteArray()) }
                stream.closeSend()
            } catch (error: Throwable) {
                if (error is CancellationException && currentCoroutineContext().isActive) {
                    stream.abort(WsStatusException(1, "request source cancelled", cause = error))
                } else if (error !is CancellationException) {
                    synchronized(lock) { pumpError = error }
                    stream.cancel()
                }
            }
        }
        val caller = currentCoroutineContext().job.invokeOnCompletion { error ->
            if (error != null) stream.abort(WsStatusException(1, "call cancelled", cause = error))
        }
        stream.onClose { pump.cancel(); caller.dispose() }
        pump.start()
        val collected = AtomicBoolean(false)
        val messages = flow {
            check(collected.compareAndSet(false, true)) { "response flow may only be collected once" }
            var readFailed = false
            try {
                request.options.onHeader?.invoke(stream.header.await())
                while (true) {
                    val bytes = stream.recv() ?: break
                    emit(request.method.output.parseFrom(bytes))
                }
            } catch (error: Throwable) {
                readFailed = true
                throw error
            } finally {
                stream.cancel()
                pump.cancel()
                request.options.onTrailer?.invoke(stream.trailer.await())
                if (!readFailed) synchronized(lock) { pumpError }?.let { throw it }
            }
        }
        return StreamResponse(messages, stream.header, stream.trailer) { stream.cancel(); pump.cancel() }
    }

    override fun close() = synchronized(lock) {
        if (closed) return@synchronized
        closed = true
        retry?.cancel()
        dialTimer?.cancel()
        stableTimer?.cancel()
        connection?.terminate(WsStatusException(1, "transport closed"), false, notify = false)
        mutableState.value = ConnectionState.Closed
        owner.cancel()
    }

    private inner class Connection : WsSocketListener {
        var socket: WsSocket? = null
        var open = false
        var terminal: WsStatusException? = null
        private var nextId = 1L
        private val streams = mutableMapOf<Int, ClientStream>()
        private val pending = ArrayDeque<ByteArray>()
        private var pendingBytes = 0L

        override fun onOpen(protocol: String) = synchronized(lock) {
            if (terminal != null || connection !== this) return@synchronized
            if (protocol != SUBPROTOCOL) {
                terminate(WsStatusException(14, "server did not negotiate subprotocol \"$SUBPROTOCOL\" (got \"$protocol\")"), false)
                return@synchronized
            }
            open = true
            dialTimer?.cancel()
            while (pending.isNotEmpty() && terminal == null) send(pending.removeFirst())
            pendingBytes = 0
            if (terminal != null) return@synchronized
            if (options.backoff.stableMs <= 0) attempt = 0 else {
                stableTimer = scope.launch {
                    delay(options.backoff.stableMs)
                    synchronized(lock) { if (connection === this@Connection && terminal == null) attempt = 0 }
                }
            }
            mutableState.value = ConnectionState.Connected
        }

        override fun onFailure(cause: Throwable?) = synchronized(lock) {
            terminate(WsStatusException(14, "connection lost", cause = cause), true)
        }

        fun openStream(method: String, callOptions: CallOptions): ClientStream {
            val id = nextId.toInt()
            nextId += 2
            val stream = ClientStream(id, lock, initialWindow, maxSendBuffer, ::write) { streams.remove(id) }
            terminal?.let { stream.finish(it); return stream }
            if (nextId > 0x1_0000_0001L) {
                stream.finish(WsStatusException(8, "stream id space exhausted"))
                return stream
            }
            streams[id] = stream
            val headers = callOptions.headers.toMutableMap()
            if (callOptions.timeoutMs > 0) headers["ws-timeout-ms"] = callOptions.timeoutMs.toString()
            write(Frame.newBuilder().setStreamId(id).setKind(Kind.KIND_OPEN).setMethod(method).putAllHeaders(headers).build())
            if (callOptions.timeoutMs > 0 && !stream.finished) {
                val deadline = scope.launch {
                    delay(callOptions.timeoutMs)
                    stream.abort(WsStatusException(4, "deadline exceeded"))
                }
                stream.onClose { deadline.cancel() }
            }
            return stream
        }

        fun write(frame: Frame) {
            if (terminal != null) return
            val bytes = frame.toByteArray()
            if (open) send(bytes) else {
                if (pendingBytes + bytes.size > 4 * 1024 * 1024) {
                    terminate(WsStatusException(14, "send buffer overflow before connect"), false)
                    return
                }
                pending.addLast(bytes)
                pendingBytes += bytes.size
            }
        }

        private fun send(bytes: ByteArray) {
            try {
                if (socket?.send(bytes) != true) terminate(WsStatusException(14, "connection lost"), true)
            } catch (error: Exception) {
                terminate(WsStatusException(14, "connection lost", cause = error), true)
            }
        }

        override fun onMessage(bytes: ByteArray) = synchronized(lock) {
            if (terminal != null || connection !== this) return@synchronized
            if (options.maxFrameBytes > 0 && bytes.size > options.maxFrameBytes) {
                terminate(WsStatusException(8, "inbound frame exceeds maxFrameBytes"), false)
                return@synchronized
            }
            val frame = try { Frame.parseFrom(bytes) } catch (_: com.google.protobuf.InvalidProtocolBufferException) { return@synchronized }
            val stream = streams[frame.streamId]
            when (frame.kind) {
                Kind.KIND_HEADER -> stream?.leading(frame.headersMap)
                Kind.KIND_MSG -> stream?.push(frame.payload.toByteArray())
                Kind.KIND_WINDOW_UPDATE -> stream?.credit(frame.window)
                Kind.KIND_END -> stream?.finish(statusError(frame.status), frame.headersMap)
                Kind.KIND_RST -> stream?.finish(statusError(frame.status) ?: WsStatusException(1, frame.status.message.ifEmpty { "stream reset" }), frame.headersMap)
                Kind.KIND_CONN_END -> if (frame.streamId == 0) terminate(statusError(frame.status) ?: WsStatusException(14, "connection ended by server"), false)
                else -> Unit
            }
        }

        fun terminate(error: WsStatusException, redial: Boolean, notify: Boolean = true) {
            if (terminal != null) return
            terminal = error
            open = false
            pending.clear()
            pendingBytes = 0
            streams.values.toList().forEach { it.finish(error) }
            try { socket?.close() } catch (_: Exception) { /* The terminal cause is already retained. */ }
            if (notify && connection === this) dropped(error, redial)
        }
    }
}

private fun statusError(status: Status): WsStatusException? =
    if (status.code == 0) null else WsStatusException(status.code, status.message, status.detailsList.toList())
