package expo.modules.t3quic

import android.os.Build
import android.os.Process
import androidx.annotation.Keep
import expo.modules.kotlin.Promise
import expo.modules.kotlin.modules.Module
import expo.modules.kotlin.modules.ModuleDefinition
import expo.modules.kotlin.records.Field
import expo.modules.kotlin.records.Record

@Keep
class T3QuicModule : Module() {
  @Keep
  interface Completion {
    fun complete(status: Int, handle: Long, data: ByteArray)
  }

  @Keep
  private class Result(
    @Field val status: Int,
    @Field val handle: Double = 0.0,
    @Field val data: ByteArray = byteArrayOf()
  ) : Record

  private data class Operation(val owner: Long, val id: Long)

  private val lock = Any()
  private val owners = mutableSetOf<Long>()
  private val pending = mutableMapOf<Operation, Pending>()
  private var destroyed = false

  private inner class Pending(val operation: Operation, val kind: Int, val promise: Promise) : Completion {
    override fun complete(status: Int, handle: Long, data: ByteArray) {
      val removed = synchronized(lock) {
        if (pending[operation] !== this) false else {
          pending.remove(operation)
          true
        }
      }
      if (removed) promise.resolve(Result(status, handle.toDouble(), data))
    }
  }

  override fun definition() = ModuleDefinition {
    Name("T3Quic")

    Function("isAvailable") { available }

    Function("createOwner") {
      synchronized(lock) {
        if (!available || destroyed) 0.0 else {
          val owner = nativeCreateOwner()
          if (owner > 0) owners.add(owner)
          owner.toDouble()
        }
      }
    }

    AsyncFunction("submit") {
        owner: Double, operation: Double, kind: Int, handle: Double,
        data: ByteArray, argument: Int, promise: Promise ->
      submit(owner, operation, kind, handle, data, argument, promise)
    }

    Function("cancel") { owner: Double, operation: Double ->
      if (!isHandle(owner) || !isHandle(operation)) return@Function
      val completion = synchronized(lock) {
        val key = Operation(owner.toLong(), operation.toLong())
        pending[key]?.also { nativeCancel(key.owner, key.id) }
      }
      completion?.complete(STATUS_ABORTED, 0, byteArrayOf())
    }

    Function("close") { owner: Double, code: Double ->
      if (isHandle(owner) && isCode(code)) release(owner.toLong(), code.toLong().toInt())
    }

    Function("reset") { owner: Double, stream: Double, code: Double ->
      if (!isHandle(owner) || !isHandle(stream) || !isCode(code)) return@Function
      synchronized(lock) {
        if (owner.toLong() in owners) nativeReset(owner.toLong(), stream.toLong(), code.toLong().toInt())
      }
    }

    Function("stop") { owner: Double, stream: Double, code: Double ->
      if (!isHandle(owner) || !isHandle(stream) || !isCode(code)) return@Function
      synchronized(lock) {
        if (owner.toLong() in owners) nativeStop(owner.toLong(), stream.toLong(), code.toLong().toInt())
      }
    }

    OnDestroy {
      val owned = synchronized(lock) {
        destroyed = true
        owners.toList()
      }
      owned.forEach { release(it, 0) }
    }
  }

  private fun submit(
    owner: Double, operation: Double, kind: Int, handle: Double,
    data: ByteArray, argument: Int, promise: Promise
  ) {
    if (!isHandle(owner) || !isHandle(operation) || !isHandle(handle, allowZero = true)) {
      promise.resolve(Result(STATUS_INVALID))
      return
    }
    val key = Operation(owner.toLong(), operation.toLong())
    synchronized(lock) {
      if (!available || destroyed || key.owner !in owners) {
        promise.resolve(Result(if (!available) STATUS_UNAVAILABLE else STATUS_ABORTED))
        return
      }
      if (pending.containsKey(key)) {
        promise.resolve(Result(STATUS_CAPACITY))
        return
      }
      val completion = Pending(key, kind, promise)
      pending[key] = completion
      val status = nativeSubmit(key.owner, key.id, kind, handle.toLong(), data, argument, completion)
      if (status < 0) completion.complete(status, 0, byteArrayOf())
    }
  }

  // Drop wrapper ownership before native close, which cancels blocked work without waiting.
  private fun release(owner: Long, code: Int) {
    val completions = synchronized(lock) {
      if (!owners.remove(owner)) return
      nativeClose(owner, code)
      pending.filterKeys { it.owner == owner }.values.toList()
    }
    completions.forEach {
      if (it.kind == KIND_CLOSED) {
        it.complete(STATUS_CLEAN_CLOSED, (code.toLong() and 0xffffffffL) + 1, byteArrayOf())
      } else {
        it.complete(STATUS_ABORTED, 0, byteArrayOf())
      }
    }
  }

  private fun isHandle(value: Double, allowZero: Boolean = false) =
    value.isFinite() && value >= (if (allowZero) 0.0 else 1.0) &&
      value <= 9007199254740991.0 && value == value.toLong().toDouble()

  private fun isCode(value: Double) =
    value.isFinite() && value >= 0.0 && value <= 4294967295.0 && value == value.toLong().toDouble()

  private external fun nativeCreateOwner(): Long
  private external fun nativeSubmit(
    owner: Long, operation: Long, kind: Int, handle: Long,
    data: ByteArray, argument: Int, completion: Completion
  ): Int
  private external fun nativeCancel(owner: Long, operation: Long)
  private external fun nativeClose(owner: Long, code: Int)
  private external fun nativeReset(owner: Long, stream: Long, code: Int)
  private external fun nativeStop(owner: Long, stream: Long, code: Int)

  companion object {
    private const val STATUS_ABORTED = -1
    private const val STATUS_INVALID = -4
    private const val STATUS_CAPACITY = -5
    private const val STATUS_UNAVAILABLE = -6
    private const val STATUS_CLEAN_CLOSED = 2
    private const val KIND_CLOSED = 6

    private val available by lazy {
      // A 64-bit device can still run a 32-bit process. Check before touching JNI.
      if (!Process.is64Bit() || Build.SUPPORTED_ABIS.none { it == "arm64-v8a" || it == "x86_64" }) {
        false
      } else {
        try {
          System.loadLibrary("t3_quic")
          true
        } catch (_: UnsatisfiedLinkError) {
          false
        } catch (_: SecurityException) {
          false
        }
      }
    }
  }
}
