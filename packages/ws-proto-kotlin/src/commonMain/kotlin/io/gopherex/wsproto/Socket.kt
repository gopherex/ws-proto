package io.gopherex.wsproto

import io.ktor.client.HttpClient
import io.ktor.client.plugins.websocket.webSocketSession
import io.ktor.http.HttpHeaders
import io.ktor.websocket.Frame
import kotlinx.atomicfu.atomic
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch

interface WsSocket {
    fun send(bytes: ByteArray): Boolean
    fun close()
}

interface WsSocketListener {
    fun onOpen(protocol: String)
    fun onMessage(bytes: ByteArray)
    fun onFailure(cause: Throwable?)
}

fun interface WsSocketFactory {
    fun connect(url: String, headers: Map<String, String>, listener: WsSocketListener): WsSocket
}

/** Inject a client with the WebSockets plugin installed to customize TLS and networking. */
class KtorSocketFactory(
    private val client: HttpClient = sharedClient,
    private val maxBufferedBytes: Int = 16 * 1024 * 1024,
) : WsSocketFactory {
    init { require(maxBufferedBytes > 0) }

    override fun connect(url: String, headers: Map<String, String>, listener: WsSocketListener): WsSocket =
        KtorSocket(client, url, headers, listener, maxBufferedBytes)

    companion object {
        private val sharedClient by lazy { defaultHttpClient() }
    }
}

internal expect fun defaultHttpClient(): HttpClient
internal expect fun currentTimeMillis(): Long

private class KtorSocket(
    client: HttpClient,
    url: String,
    upgradeHeaders: Map<String, String>,
    listener: WsSocketListener,
    private val maxBufferedBytes: Int,
) : WsSocket {
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    private val outgoing = Channel<ByteArray>(Channel.UNLIMITED)
    private val queuedBytes = atomic(0L)
    private val closed = atomic(false)

    init {
        scope.launch {
            try {
                val session = client.webSocketSession(urlString = url) {
                    upgradeHeaders.forEach { (name, value) -> this.headers[name] = value }
                    this.headers[HttpHeaders.SecWebSocketProtocol] = SUBPROTOCOL
                }
                try {
                    listener.onOpen(session.call.response.headers[HttpHeaders.SecWebSocketProtocol] ?: "")
                    coroutineScope {
                        val writer = launch {
                            for (bytes in outgoing) {
                                session.send(Frame.Binary(true, bytes))
                                queuedBytes.addAndGet(-bytes.size.toLong())
                            }
                        }
                        try {
                            for (frame in session.incoming) {
                                if (frame is Frame.Binary) listener.onMessage(frame.data)
                            }
                        } finally { writer.cancel() }
                    }
                } finally { session.cancel() }
                listener.onFailure(null)
            } catch (error: Throwable) {
                listener.onFailure(error)
            } finally {
                closed.value = true
                outgoing.cancel()
                scope.cancel()
            }
        }
    }

    override fun send(bytes: ByteArray): Boolean {
        if (closed.value) return false
        while (true) {
            val old = queuedBytes.value
            if (old + bytes.size > maxBufferedBytes) return false
            if (queuedBytes.compareAndSet(old, old + bytes.size)) break
        }
        if (outgoing.trySend(bytes).isSuccess) return true
        queuedBytes.addAndGet(-bytes.size.toLong())
        return false
    }

    override fun close() {
        if (closed.compareAndSet(false, true)) {
            outgoing.cancel()
            scope.cancel()
        }
    }
}
