package com.sd.lib.websocket

import android.annotation.SuppressLint
import com.sd.lib.websocket.WSocket.ConnectionState
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.StateFlow
import okhttp3.OkHttpClient
import java.security.SecureRandom
import java.security.cert.X509Certificate
import java.util.concurrent.TimeUnit
import javax.net.ssl.SSLContext
import javax.net.ssl.X509TrustManager

interface WSocket {
  /** 连接状态 */
  val connectionStateFlow: StateFlow<ConnectionState>

  /** 消息 */
  val messageFlow: Flow<String>

  /** 连接 */
  @Throws(WSocketException::class)
  suspend fun connect()

  /** 发送消息 */
  fun send(text: String): Boolean

  /** 断开连接 */
  fun disconnect()

  /** 连接状态 */
  enum class ConnectionState {
    /** 未连接 */
    Idle,
    /** 连接中 */
    Connecting,
    /** 已连接 */
    Connected,
  }

  companion object {
    fun create(
      url: String,
      okHttpClientProvider: () -> OkHttpClient = { SingletonOkhttpClient },
    ): WSocket {
      require(url.isNotBlank())
      return OkhttpWSocket(
        url = url,
        okHttpClientProvider = okHttpClientProvider,
      )
    }
  }
}

fun WSocket.getConnectionState(): ConnectionState {
  return connectionStateFlow.value
}

private val SingletonOkhttpClient: OkHttpClient by lazy {
  @SuppressLint("CustomX509TrustManager")
  val trustManager = object : X509TrustManager {
    override fun checkClientTrusted(chain: Array<out X509Certificate>?, type: String?) = Unit
    override fun checkServerTrusted(chain: Array<out X509Certificate>?, type: String?) = Unit
    override fun getAcceptedIssuers(): Array<X509Certificate> = emptyArray()
  }

  val sslContext = SSLContext.getInstance("TLS").apply {
    init(null, arrayOf(trustManager), SecureRandom())
  }

  OkHttpClient.Builder()
    .pingInterval(10_000, TimeUnit.MILLISECONDS)
    .sslSocketFactory(sslContext.socketFactory, trustManager)
    .hostnameVerifier { _, _ -> true }
    .build()
}