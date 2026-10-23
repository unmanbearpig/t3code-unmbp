//! JNI only copies bounded buffers and dispatches to the shared async runtime.
use super::*;
use jni::{
    JNIEnv,
    objects::{JByteArray, JObject, JValue},
    sys::{jint, jlong},
};

#[unsafe(no_mangle)]
pub extern "system" fn Java_expo_modules_t3quic_T3QuicModule_nativeCreateOwner(
    _: JNIEnv,
    _: JObject,
) -> jlong {
    t3_quic_owner_create() as jlong
}
#[unsafe(no_mangle)]
pub extern "system" fn Java_expo_modules_t3quic_T3QuicModule_nativeSubmit(
    env: JNIEnv,
    _: JObject,
    owner: jlong,
    operation: jlong,
    kind: jint,
    handle: jlong,
    data: JByteArray,
    argument: jint,
    completion: JObject,
) -> jint {
    catch_unwind(AssertUnwindSafe(|| {
        let length = match env.get_array_length(&data) {
            Ok(length) => length as usize,
            Err(_) => return PROTOCOL,
        };
        if length > MAX_BYTES {
            return CAPACITY;
        }
        let data = match env.convert_byte_array(data) {
            Ok(data) => data,
            Err(_) => return PROTOCOL,
        };
        let vm = match env.get_java_vm() {
            Ok(vm) => vm,
            Err(_) => return UNAVAILABLE,
        };
        let completion = match env.new_global_ref(completion) {
            Ok(completion) => completion,
            Err(_) => return UNAVAILABLE,
        };
        submit(
            owner as u64,
            operation as u64,
            kind as u32,
            handle as u64,
            data,
            argument as u32,
            Box::new(move |value| {
                if let Ok(mut env) = vm.attach_current_thread()
                    && let Ok(bytes) = env.byte_array_from_slice(&value.bytes)
                {
                    let object = JObject::from(bytes);
                    let _ = env.call_method(
                        completion.as_obj(),
                        "complete",
                        "(IJ[B)V",
                        &[
                            JValue::Int(value.status),
                            JValue::Long(value.handle as jlong),
                            JValue::Object(&object),
                        ],
                    );
                    if env.exception_check().unwrap_or(false) {
                        let _ = env.exception_clear();
                    }
                }
            }),
        )
    }))
    .unwrap_or(PROTOCOL)
}
#[unsafe(no_mangle)]
pub extern "system" fn Java_expo_modules_t3quic_T3QuicModule_nativeCancel(
    _: JNIEnv,
    _: JObject,
    owner: jlong,
    operation: jlong,
) {
    t3_quic_cancel(owner as u64, operation as u64);
}
#[unsafe(no_mangle)]
pub extern "system" fn Java_expo_modules_t3quic_T3QuicModule_nativeClose(
    _: JNIEnv,
    _: JObject,
    owner: jlong,
    code: jint,
) {
    t3_quic_close(owner as u64, code as u32);
}
#[unsafe(no_mangle)]
pub extern "system" fn Java_expo_modules_t3quic_T3QuicModule_nativeReset(
    _: JNIEnv,
    _: JObject,
    owner: jlong,
    stream: jlong,
    code: jint,
) {
    t3_quic_reset(owner as u64, stream as u64, code as u32);
}
#[unsafe(no_mangle)]
pub extern "system" fn Java_expo_modules_t3quic_T3QuicModule_nativeStop(
    _: JNIEnv,
    _: JObject,
    owner: jlong,
    stream: jlong,
    code: jint,
) {
    t3_quic_stop(owner as u64, stream as u64, code as u32);
}
