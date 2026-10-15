package dev.t3.quicprobe;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

/** Exercises the exact JNI entrypoint on a host JVM. It is not an Android device test. */
public final class ProbeMain {
    public static void main(String[] args) throws Exception {
        String roots = new String(Files.readAllBytes(Paths.get(args[1])), StandardCharsets.UTF_8);
        String result = NativeProbe.run(args[0], roots, System.getenv("T3_QUIC_PROBE_TOKEN"), 15000);
        System.out.println(result);
        if (!result.contains("\"ok\":true")) System.exit(1);
    }
}
