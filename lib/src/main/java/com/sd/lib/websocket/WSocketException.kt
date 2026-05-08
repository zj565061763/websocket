package com.sd.lib.websocket

open class WSocketException internal constructor(
  message: String? = null,
  cause: Throwable? = null,
) : Exception(message, cause)