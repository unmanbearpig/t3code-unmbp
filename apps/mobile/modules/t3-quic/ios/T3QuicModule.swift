import ExpoModulesCore
import Foundation

private let maximumJavaScriptInteger: UInt64 = 9_007_199_254_740_991

private func nativeIdentifier(_ value: Double, allowZero: Bool = false) -> UInt64? {
  guard value.isFinite, value >= (allowZero ? 0 : 1),
    value <= Double(maximumJavaScriptInteger), value.rounded(.towardZero) == value
  else {
    return nil
  }
  return UInt64(value)
}

private func resolve(_ promise: Promise, status: Int32, handle: UInt64 = 0, data: Data = Data()) {
  promise.resolve(["status": Int(status), "handle": Double(handle), "data": data])
}

// A callback can outlive its Expo module. Remove the promise under the lock and
// settle it afterward, since settlement can schedule work in the JS runtime.
private final class T3QuicCallbacks: @unchecked Sendable {
  static let shared = T3QuicCallbacks()

  private let lock = NSLock()
  private var nextContext: UInt64 = 1
  private var promises: [UInt64: Promise] = [:]

  func insert(_ promise: Promise) -> UInt64 {
    lock.withLock {
      while nextContext == 0 || promises[nextContext] != nil {
        nextContext &+= 1
      }
      let context = nextContext
      nextContext &+= 1
      promises[context] = promise
      return context
    }
  }

  func take(_ context: UInt64) -> Promise? {
    lock.withLock { promises.removeValue(forKey: context) }
  }
}

private let quicCallback: T3QuicCallback = { context, _, status, handle, bytes, length in
  let data: Data
  if let bytes, length > 0 {
    data = Data(bytes: bytes, count: length)
  } else {
    data = Data()
  }
  t3_quic_buffer_free(bytes, length)

  if let promise = T3QuicCallbacks.shared.take(context) {
    resolve(promise, status: status, handle: handle, data: data)
  }
}

public final class T3QuicModule: Module {
  private let ownerLock = NSLock()
  private var owners = Set<UInt64>()
  private var destroyed = false

  public func definition() -> ModuleDefinition {
    Name("T3Quic")

    Function("isAvailable") { true }

    Function("createOwner") { () -> Double in
      let owner = t3_quic_owner_create()
      guard owner > 0 else { return 0 }
      let retained = self.ownerLock.withLock {
        if self.destroyed || owner > maximumJavaScriptInteger { return false }
        self.owners.insert(owner)
        return true
      }
      if !retained {
        t3_quic_close(owner, 0)
        return 0
      }
      return Double(owner)
    }

    AsyncFunction("submit") {
      (ownerValue: Double, operationValue: Double, kind: Int, handleValue: Double,
       data: Data, argument: Int, promise: Promise) in
      guard let owner = nativeIdentifier(ownerValue),
        let operation = nativeIdentifier(operationValue),
        let handle = nativeIdentifier(handleValue, allowZero: true),
        (1...6).contains(kind), let nativeArgument = UInt32(exactly: argument),
        kind != 3 || (1...65_536).contains(argument),
        kind != 4 || data.count <= 65_536
      else {
        resolve(promise, status: -4)
        return
      }
      guard self.owns(owner) else {
        resolve(promise, status: -1)
        return
      }

      let context = T3QuicCallbacks.shared.insert(promise)
      let status = data.withUnsafeBytes { bytes in
        t3_quic_submit(owner, operation, UInt32(kind), handle,
                       bytes.bindMemory(to: UInt8.self).baseAddress, bytes.count,
                       nativeArgument, quicCallback, context)
      }
      if status != 0, let pending = T3QuicCallbacks.shared.take(context) {
        resolve(pending, status: status < 0 ? status : -4)
      }
    }

    Function("cancel") { (ownerValue: Double, operationValue: Double) in
      guard let owner = nativeIdentifier(ownerValue),
        let operation = nativeIdentifier(operationValue), self.owns(owner)
      else { return }
      t3_quic_cancel(owner, operation)
    }

    Function("close") { (ownerValue: Double, code: Int) in
      guard let owner = nativeIdentifier(ownerValue), let nativeCode = UInt32(exactly: code)
      else { return }
      let removed = self.ownerLock.withLock { self.owners.remove(owner) != nil }
      if removed { t3_quic_close(owner, nativeCode) }
    }

    Function("reset") { (ownerValue: Double, streamValue: Double, code: Int) in
      guard let owner = nativeIdentifier(ownerValue),
        let stream = nativeIdentifier(streamValue, allowZero: true),
        let nativeCode = UInt32(exactly: code), self.owns(owner)
      else { return }
      t3_quic_reset(owner, stream, nativeCode)
    }

    Function("stop") { (ownerValue: Double, streamValue: Double, code: Int) in
      guard let owner = nativeIdentifier(ownerValue),
        let stream = nativeIdentifier(streamValue, allowZero: true),
        let nativeCode = UInt32(exactly: code), self.owns(owner)
      else { return }
      t3_quic_stop(owner, stream, nativeCode)
    }

    OnDestroy {
      let remaining = self.ownerLock.withLock {
        self.destroyed = true
        let remaining = self.owners
        self.owners.removeAll()
        return remaining
      }
      for owner in remaining { t3_quic_close(owner, 0) }
    }
  }

  private func owns(_ owner: UInt64) -> Bool {
    ownerLock.withLock { !destroyed && owners.contains(owner) }
  }
}
