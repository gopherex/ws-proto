package io.gopherex.wsproto

import com.squareup.wire.ProtoAdapter
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.test.runTest
import wiretest.`when`.PresenceMessage as Presence
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertNull

class ProtobufTest {
    @Test fun optionalPresenceAndUnknownEnumSurviveEncoding() {
        val absent = Presence()
        val present = Presence(text = "")
        assertNull(Presence.ADAPTER.decode(Presence.ADAPTER.encode(absent)).text)
        assertEquals("", Presence.ADAPTER.decode(Presence.ADAPTER.encode(present)).text)
        val unknownEnum = byteArrayOf(0x10, 0x7f)
        assertContentEquals(unknownEnum, Presence.ADAPTER.encode(Presence.ADAPTER.decode(unknownEnum)))
    }

    @Test fun clientStreamingCanReturnNullWellKnownValue() = runTest {
        val response = StreamResponse<String?>(flowOf(null), CompletableDeferred(emptyMap()), CompletableDeferred(emptyMap()), {})
        assertNull(firstResponse(response))
    }

    @Test fun nullableWellKnownAdaptersAreValidMethodCodecs() {
        val method = MethodInfo<String?, String?>("test.Wrappers", "Call", MethodKind.UNARY,
            ProtoAdapter.STRING_VALUE, ProtoAdapter.STRING_VALUE)
        assertEquals("hello", method.output.decode(method.input.encode("hello")))
    }
}
