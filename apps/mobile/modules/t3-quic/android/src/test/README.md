Run the host wrapper checks after compiling the Android module against the installed Expo version:

```sh
JAVA_HOME=/path/to/jdk python3 src/test/run-wrapper-tests.py "$T3_QUIC_WRAPPER_CLASSPATH"
```

The classpath must contain the module's compiled Kotlin classes and its resolved Gradle `debugRuntimeClasspath`. The runner extracts AAR classes, builds a controlled JNI fixture with the host C compiler, and removes its temporary directory afterward. It needs Linux, a JDK, Python 3, and a C compiler. No additional Gradle dependencies are required.

The checks execute the compiled module's exported Expo function bodies. They hold the submit/cancel FIFO and native callbacks separately to reproduce cancellation before dispatch and cancellation after a successful native open has been published. They verify terminal Promise settlement, delivery of the late stream handle for reset/stop cleanup, blocked-operation cleanup, synchronous close before queued submission, duplicate callbacks, full u32 close codes, and the process-width guard before JNI loading.

The held FIFO follows Expo 58's `AsyncFunctionWithPromiseComponent` and `Queues.DEFAULT` scheduling. When updating Expo, recheck that both non-suspending bodies dispatch to the same serial modules queue. These tests use a controlled native boundary and Android platform stubs; they do not run Rust, Android's HandlerThread, React, or the JavaScript connector.
