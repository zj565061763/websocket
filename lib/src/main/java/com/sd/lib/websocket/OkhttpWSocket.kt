package com.sd.lib.websocket

import com.sd.lib.websocket.WSocket.ConnectionState
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asSharedFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.launch
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.Response
import okhttp3.WebSocket
import okhttp3.WebSocketListener

internal class OkhttpWSocket(
  private val url: String,
  private val okHttpClientProvider: () -> OkHttpClient,
) : WSocket {
  private val _lock = Any()
  private var _websocket: WebSocket? = null

  private var _connectDeferred: CompletableDeferred<Unit>? = null
  private var _listener: OkHttpWebSocketListener? = null
  @Volatile
  private var _coroutineScope: CoroutineScope? = null

  private val _connectionStateFlow: MutableStateFlow<ConnectionState> = MutableStateFlow(ConnectionState.Idle)
  private val _messageFlow: MutableSharedFlow<String> = MutableSharedFlow()

  override val connectionStateFlow: StateFlow<ConnectionState> = _connectionStateFlow.asStateFlow()
  override val messageFlow: Flow<String> = _messageFlow.asSharedFlow()

  override suspend fun connect() {
    synchronized(_lock) {
      when (getConnectionState()) {
        ConnectionState.Idle -> doConnect()
        ConnectionState.Connecting -> _connectDeferred
        ConnectionState.Connected -> null
      }
    }?.await()
  }

  override fun send(text: String): Boolean {
    return _websocket?.send(text) == true
  }

  override fun disconnect() {
    synchronized(_lock) {
      _connectionStateFlow.value = ConnectionState.Idle
      _connectDeferred?.cancel()
      _connectDeferred = null
      _listener?.destroy()
      _listener = null
      _coroutineScope?.cancel()
      _coroutineScope = null
      _websocket?.close(1000, "")
      _websocket = null
    }
  }

  private fun doConnect(): CompletableDeferred<Unit> {
    return CompletableDeferred<Unit>().also { deferred ->
      _connectionStateFlow.value = ConnectionState.Connecting
      _connectDeferred = deferred

      val request = Request.Builder()
        .url(url)
        .build()

      okHttpClientProvider().newWebSocket(
        request, OkHttpWebSocketListener(
          lock = _lock,
          callbackOnOpen = { webSocket ->
            _websocket = webSocket
            _connectionStateFlow.value = ConnectionState.Connected
            deferred.complete(Unit)
          },
          callbackOnMessage = { text ->
            getCoroutineScope()?.launch { _messageFlow.emit(text) }
          },
          callbackOnClosed = {
            _websocket = null
            disconnect()
          },
          callbackOnFailure = { t ->
            deferred.completeExceptionally(WSocketException(t.message, t))
            disconnect()
          },
        ).also { _listener = it }
      )
    }
  }

  @OptIn(ExperimentalCoroutinesApi::class)
  private fun getCoroutineScope(): CoroutineScope? {
    _coroutineScope?.also { return it }
    synchronized(_lock) {
      _coroutineScope?.also { return it }
      if (getConnectionState() == ConnectionState.Idle) return null
      return CoroutineScope(SupervisorJob() + Dispatchers.IO.limitedParallelism(1))
        .also { _coroutineScope = it }
    }
  }
}

private class OkHttpWebSocketListener(
  private val lock: Any,
  private val callbackOnOpen: (webSocket: WebSocket) -> Unit,
  private val callbackOnMessage: (text: String) -> Unit,
  private val callbackOnClosed: () -> Unit,
  private val callbackOnFailure: (Throwable) -> Unit,
) : WebSocketListener() {
  @Volatile
  private var _destroyed = false

  override fun onOpen(webSocket: WebSocket, response: Response) {
    synchronized(lock) {
      if (!_destroyed) {
        callbackOnOpen(webSocket)
      }
    }
  }

  override fun onMessage(webSocket: WebSocket, text: String) {
    if (!_destroyed) {
      callbackOnMessage(text)
    }
  }

  override fun onClosed(webSocket: WebSocket, code: Int, reason: String) {
    synchronized(lock) {
      if (!_destroyed) {
        callbackOnClosed()
      }
    }
  }

  override fun onFailure(webSocket: WebSocket, t: Throwable, response: Response?) {
    synchronized(lock) {
      if (!_destroyed) {
        callbackOnFailure(t)
      }
    }
  }

  fun destroy() {
    _destroyed = true
  }
}
