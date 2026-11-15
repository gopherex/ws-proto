package io.gopherex.wsproto

/** Integration suites require the launcher to supply a live Go endpoint. */
internal expect fun testServerUrl(): String
