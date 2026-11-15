package io.gopherex.wsproto

import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.Response
import okhttp3.WebSocket
import okhttp3.WebSocketListener
import okio.ByteString
import okio.ByteString.Companion.toByteString

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

/** The caller owns an injected OkHttpClient; closing the transport closes only its WebSocket. */
class OkHttpSocketFactory(private val client: OkHttpClient = sharedClient) : WsSocketFactory {
    override fun connect(url: String, headers: Map<String, String>, listener: WsSocketListener): WsSocket {
        val request = Request.Builder().url(url).apply {
            headers.forEach { (name, value) -> header(name, value) }
            header("Sec-WebSocket-Protocol", SUBPROTOCOL)
        }.build()
        val socket = client.newWebSocket(request, object : WebSocketListener() {
            override fun onOpen(webSocket: WebSocket, response: Response) {
                listener.onOpen(response.header("Sec-WebSocket-Protocol") ?: "")
            }
            override fun onMessage(webSocket: WebSocket, bytes: ByteString) = listener.onMessage(bytes.toByteArray())
            override fun onClosing(webSocket: WebSocket, code: Int, reason: String) {
                webSocket.close(code, reason)
                listener.onFailure(null)
            }
            override fun onClosed(webSocket: WebSocket, code: Int, reason: String) = listener.onFailure(null)
            override fun onFailure(webSocket: WebSocket, t: Throwable, response: Response?) = listener.onFailure(t)
        })
        return object : WsSocket {
            override fun send(bytes: ByteArray) = socket.send(bytes.toByteString())
            override fun close() { socket.cancel() }
        }
    }

    companion object {
        private val sharedClient by lazy { OkHttpClient() }
    }
}
