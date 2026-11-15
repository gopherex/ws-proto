package io.gopherex.wsproto

import wsproto.transport.v1.Frame

internal class FakeSocketFactory : WsSocketFactory {
    val frames = mutableListOf<Frame>()
    lateinit var listener: WsSocketListener
    var closed = false
    var connects = 0
    override fun connect(url: String, headers: Map<String, String>, listener: WsSocketListener): WsSocket {
        this.listener = listener
        closed = false
        connects++
        return object : WsSocket {
            override fun send(bytes: ByteArray): Boolean { frames.add(Frame.ADAPTER.decode(bytes)); return !closed }
            override fun close() { closed = true }
        }
    }
    fun open(protocol: String = SUBPROTOCOL) = listener.onOpen(protocol)
    fun inject(frame: Frame) = listener.onMessage(Frame.ADAPTER.encode(frame))
    fun drop() = listener.onFailure(null)
}
