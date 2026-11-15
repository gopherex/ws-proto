package io.gopherex.wsproto.consumer

import io.gopherex.wsproto.CallOptions
import io.gopherex.wsproto.MethodInfo
import io.gopherex.wsproto.WsTransport
import com.google.protobuf.MessageLite

/** Compiles the public suspend API through Android's compiler and dex pipeline. */
suspend fun <I : MessageLite, O : MessageLite> call(
    transport: WsTransport,
    method: MethodInfo<I, O>,
    request: I,
): O = transport.unary(method, request, CallOptions(timeoutMs = 5_000)).message
