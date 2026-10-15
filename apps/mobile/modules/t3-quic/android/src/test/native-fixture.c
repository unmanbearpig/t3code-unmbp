#include <jni.h>
#include <stdbool.h>

// Controlled JNI boundary, not a replacement transport. A published open has already
// left the native operation map, but its terminal callback can still be held here.
static jobject completion;
static jlong operation;
static jlong stream;
static bool blocked;
static bool send_half;
static bool recv_half;
static int submit_count;
static jlong next_owner = 1;

JNIEXPORT jint JNICALL JNI_OnLoad(JavaVM *vm, void *reserved) {
  JNIEnv *env;
  if ((*vm)->GetEnv(vm, (void **)&env, JNI_VERSION_1_6) != JNI_OK) return JNI_ERR;
  jclass system = (*env)->FindClass(env, "java/lang/System");
  jmethodID set = (*env)->GetStaticMethodID(env, system, "setProperty",
      "(Ljava/lang/String;Ljava/lang/String;)Ljava/lang/String;");
  jstring key = (*env)->NewStringUTF(env, "t3.test.library.loaded");
  jstring value = (*env)->NewStringUTF(env, "true");
  (*env)->CallStaticObjectMethod(env, system, set, key, value);
  (*env)->DeleteLocalRef(env, key);
  (*env)->DeleteLocalRef(env, value);
  (*env)->DeleteLocalRef(env, system);
  return JNI_VERSION_1_6;
}

static void complete(JNIEnv *env, jint status, jlong handle) {
  jclass type = (*env)->GetObjectClass(env, completion);
  jmethodID method = (*env)->GetMethodID(env, type, "complete", "(IJ[B)V");
  jbyteArray bytes = (*env)->NewByteArray(env, 0);
  (*env)->CallVoidMethod(env, completion, method, status, handle, bytes);
  (*env)->DeleteLocalRef(env, bytes);
  (*env)->DeleteLocalRef(env, type);
}

JNIEXPORT jlong JNICALL Java_expo_modules_t3quic_T3QuicModule_nativeCreateOwner(
    JNIEnv *env, jobject self) {
  return next_owner++;
}

JNIEXPORT jint JNICALL Java_expo_modules_t3quic_T3QuicModule_nativeSubmit(
    JNIEnv *env, jobject self, jlong owner, jlong id, jint kind, jlong handle,
    jbyteArray data, jint argument, jobject callback) {
  if (completion) (*env)->DeleteGlobalRef(env, completion);
  completion = (*env)->NewGlobalRef(env, callback);
  operation = id;
  blocked = true;
  submit_count++;
  return 0;
}

JNIEXPORT void JNICALL Java_expo_modules_t3quic_T3QuicModule_nativeCancel(
    JNIEnv *env, jobject self, jlong owner, jlong id) {
  if (id == operation && blocked) {
    blocked = false;
    complete(env, -1, 0);
  }
}

JNIEXPORT void JNICALL Java_expo_modules_t3quic_T3QuicModule_nativeClose(
    JNIEnv *env, jobject self, jlong owner, jint code) {
  blocked = false;
  send_half = recv_half = false;
}

JNIEXPORT void JNICALL Java_expo_modules_t3quic_T3QuicModule_nativeReset(
    JNIEnv *env, jobject self, jlong owner, jlong handle, jint code) {
  if (handle == stream) send_half = false;
}

JNIEXPORT void JNICALL Java_expo_modules_t3quic_T3QuicModule_nativeStop(
    JNIEnv *env, jobject self, jlong owner, jlong handle, jint code) {
  if (handle == stream) recv_half = false;
}

JNIEXPORT void JNICALL Java_expo_modules_t3quic_T3QuicModuleTest_publishOpen(
    JNIEnv *env, jclass self, jlong id, jlong handle) {
  if (id == operation && blocked) {
    blocked = false;
    stream = handle;
    send_half = recv_half = true;
  }
}

JNIEXPORT void JNICALL Java_expo_modules_t3quic_T3QuicModuleTest_deliver(
    JNIEnv *env, jclass self, jlong id) {
  if (id == operation) complete(env, 0, stream);
}

JNIEXPORT jint JNICALL Java_expo_modules_t3quic_T3QuicModuleTest_blockedOperations(
    JNIEnv *env, jclass self) {
  return blocked;
}

JNIEXPORT jint JNICALL Java_expo_modules_t3quic_T3QuicModuleTest_retainedHalves(
    JNIEnv *env, jclass self) {
  return send_half + recv_half;
}

JNIEXPORT jint JNICALL Java_expo_modules_t3quic_T3QuicModuleTest_submissions(
    JNIEnv *env, jclass self) {
  return submit_count;
}

JNIEXPORT void JNICALL JNI_OnUnload(JavaVM *vm, void *reserved) {
  JNIEnv *env;
  if ((*vm)->GetEnv(vm, (void **)&env, JNI_VERSION_1_6) == JNI_OK && completion) {
    (*env)->DeleteGlobalRef(env, completion);
  }
}
