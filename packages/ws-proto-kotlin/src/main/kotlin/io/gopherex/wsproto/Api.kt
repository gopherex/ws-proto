package io.gopherex.wsproto

import com.google.protobuf.ByteString
import com.google.protobuf.MessageLite
import com.google.protobuf.Parser
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.flow.Flow

const val SUBPROTOCOL = "wsrpc.v1"

/** Status details retain the serialized google.protobuf.Any values sent by the peer. */
class WsStatusException(
    val code: Int,
    override val message: String,
    val details: List<ByteString> = emptyList(),
    cause: Throwable? = null,
) : Exception(message, cause)

enum class MethodKind { UNARY, SERVER_STREAMING, CLIENT_STREAMING, BIDI_STREAMING }

data class MethodInfo<I : MessageLite, O : MessageLite>(
    val typeName: String,
    val name: String,
    val kind: MethodKind,
    val output: Parser<O>,
) {
    val route: String get() = "/$typeName/$name"
}

data class CallOptions(
    val headers: Map<String, String> = emptyMap(),
    val timeoutMs: Long = 0,
    val onHeader: ((Map<String, String>) -> Unit)? = null,
    val onTrailer: ((Map<String, String>) -> Unit)? = null,
)

data class UnaryRequest<I : MessageLite, O : MessageLite>(
    val method: MethodInfo<I, O>,
    val message: I,
    val options: CallOptions,
)

data class StreamRequest<I : MessageLite, O : MessageLite>(
    val method: MethodInfo<I, O>,
    val message: Flow<I>,
    val options: CallOptions,
)

data class UnaryResponse<O : MessageLite>(
    val message: O,
    val header: Map<String, String>,
    val trailer: Map<String, String>,
)

class StreamResponse<O : MessageLite>(
    val message: Flow<O>,
    val header: Deferred<Map<String, String>>,
    val trailer: Deferred<Map<String, String>>,
    private val cancelCall: () -> Unit,
) {
    fun cancel() = cancelCall()
}

/** The first interceptor is outermost. Requests and responses may be replaced or short-circuited. */
interface Interceptor {
    suspend fun <I : MessageLite, O : MessageLite> unary(
        request: UnaryRequest<I, O>,
        next: suspend (UnaryRequest<I, O>) -> UnaryResponse<O>,
    ): UnaryResponse<O> = next(request)

    suspend fun <I : MessageLite, O : MessageLite> stream(
        request: StreamRequest<I, O>,
        next: suspend (StreamRequest<I, O>) -> StreamResponse<O>,
    ): StreamResponse<O> = next(request)
}

data class Backoff(val initialMs: Long = 100, val maxMs: Long = 30_000, val stableMs: Long = 0)

data class WsTransportOptions(
    val initialWindow: Int = 256 * 1024,
    val maxSendBuffer: Int = 16 * 1024 * 1024,
    val maxFrameBytes: Int = 0,
    val reconnect: Boolean = false,
    val backoff: Backoff = Backoff(),
    val dialTimeoutMs: Long = 0,
    val paused: Boolean = false,
    val headers: Map<String, String> = emptyMap(),
    val interceptors: List<Interceptor> = emptyList(),
)

sealed interface ConnectionState {
    data object Paused : ConnectionState
    data object Connecting : ConnectionState
    data object Connected : ConnectionState
    data class Waiting(val retryAt: Long) : ConnectionState
    data class Disconnected(val error: WsStatusException) : ConnectionState
    data object Closed : ConnectionState
}

/** Matches the TS client-streaming helper: drain to END and return the last response. */
suspend fun <O : MessageLite> firstResponse(response: StreamResponse<O>): O {
    var last: O? = null
    response.message.collect { last = it }
    return last ?: throw WsStatusException(13, "server closed stream without a response")
}
