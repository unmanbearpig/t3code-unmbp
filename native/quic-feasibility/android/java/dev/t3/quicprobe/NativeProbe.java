package dev.t3.quicprobe;

/** Standalone JNI probe. Call run on a worker thread, with the app's chosen trust roots. */
public final class NativeProbe {
    static {
        System.loadLibrary("t3_quic_probe");
    }

    private NativeProbe() {}

    public static native String run(String url, String rootCaPem, String bearerToken, int timeoutMs);
}
