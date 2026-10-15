package expo.modules.t3quic;

import expo.modules.kotlin.Promise;
import expo.modules.kotlin.functions.AsyncFunctionWithPromiseComponent;
import expo.modules.kotlin.functions.BaseAsyncFunctionComponent;
import expo.modules.kotlin.functions.Queues;
import expo.modules.kotlin.modules.ModuleDefinitionData;
import java.lang.reflect.Field;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import kotlin.jvm.functions.Function1;
import kotlin.jvm.functions.Function2;

/** Host-JVM regression checks against the compiled wrapper and installed Expo function bodies. */
public final class T3QuicModuleTest {
  private static native void publishOpen(long operation, long stream);
  private static native void deliver(long operation);
  private static native int blockedOperations();
  private static native int retainedHalves();
  private static native int submissions();

  private final T3QuicModule module = new T3QuicModule();
  private final ModuleDefinitionData definition = module.definition();
  private final ArrayDeque<Runnable> queue = new ArrayDeque<>();

  private static void require(boolean condition, String message) {
    if (!condition) throw new AssertionError(message);
  }

  private static Object field(Object value, String name) throws Exception {
    Field field = value.getClass().getDeclaredField(name);
    field.setAccessible(true);
    return field.get(value);
  }

  @SuppressWarnings("unchecked")
  private Object sync(String name, Object... args) throws Exception {
    Object function = definition.getSyncFunctions().get(name);
    return ((Function1<Object[], Object>) field(function, "body")).invoke(args);
  }

  @SuppressWarnings("unchecked")
  private void enqueue(String name, Capture promise, Object... args) throws Exception {
    Object function = definition.getAsyncFunctions().get(name);
    require(function instanceof AsyncFunctionWithPromiseComponent,
      name + " must use Expo's non-suspending Promise body");
    Field queueField = BaseAsyncFunctionComponent.class.getDeclaredField("queue");
    queueField.setAccessible(true);
    require(queueField.get(function) == Queues.DEFAULT, name + " must share the modules Handler queue");
    Function2<Object[], Promise, Object> body =
      (Function2<Object[], Promise, Object>) field(function, "body");
    // Expo dispatches these non-suspending bodies with Handler.post on one HandlerThread.
    // Holding this FIFO makes an abort before the submit body runs deterministic.
    queue.add(() -> body.invoke(args, promise));
  }

  private void drain() {
    while (!queue.isEmpty()) queue.remove().run();
  }

  private double owner() throws Exception {
    double owner = (Double) sync("createOwner");
    require(owner > 0, "test JNI owner must be available");
    return owner;
  }

  private void open(double owner, double operation, Capture promise) throws Exception {
    enqueue("submit", promise, owner, operation, 1, 0.0, new byte[0], 0);
  }

  private void successfulOpenAfterCancel() throws Exception {
    double owner = owner();
    Capture submit = new Capture();
    open(owner, 1, submit);
    drain();
    publishOpen(1, 101);
    require(blockedOperations() == 0 && retainedHalves() == 2, "native open must be published");

    Capture cancel = new Capture();
    enqueue("cancel", cancel, owner, 1.0);
    drain();
    require(cancel.values.size() == 1 && cancel.values.get(0) == null, "cancel must resolve itself");
    require(submit.values.isEmpty(), "cancel must preserve the native submit callback");

    deliver(1);
    require(submit.values.size() == 1, "late successful open must reach JS exactly once");
    Object result = submit.values.get(0);
    require((Integer) field(result, "status") == 0, "late success status must survive cancel");
    require((Double) field(result, "handle") == 101.0, "late success handle must survive cancel");
    // These are the two calls the connector's onLate callback makes with the delivered handle.
    sync("reset", owner, 101.0, 0.0);
    sync("stop", owner, 101.0, 0.0);
    require(retainedHalves() == 0, "the delivered late handle must allow both halves to be reclaimed");
    deliver(1);
    require(submit.values.size() == 1, "duplicate native callbacks must not settle twice");
    sync("close", owner, 0.0);
  }

  private void queuedSubmitThenCancel() throws Exception {
    double owner = owner();
    Capture submit = new Capture();
    Capture cancel = new Capture();
    open(owner, 2, submit);
    enqueue("cancel", cancel, owner, 2.0);
    require(blockedOperations() == 0 && submit.values.isEmpty(), "queue must remain held");
    drain();
    require(cancel.values.size() == 1, "queued cancel must settle");
    require(submit.values.size() == 1, "registered blocked open must receive its native abort");
    require((Integer) field(submit.values.get(0), "status") == -1, "blocked open must abort");
    require(blockedOperations() == 0 && retainedHalves() == 0, "queued abort must leave no native work");
    require(((java.util.Map<?, ?>) field(module, "pending")).isEmpty(), "aborted wrapper entry must be released");
    sync("close", owner, 0.0);
  }

  private void closeBeforeQueuedSubmit() throws Exception {
    double owner = owner();
    Capture submit = new Capture();
    int before = submissions();
    open(owner, 3, submit);
    sync("close", owner, 0.0);
    drain();
    require(submissions() == before, "synchronous close must prevent queued native work");
    require(submit.values.size() == 1, "queued submit after close must settle");
    require((Integer) field(submit.values.get(0), "status") == -1, "closed owner must abort");
  }

  private void closeCodeRange() throws Exception {
    for (double code : new double[] {0, 2147483647.0, 2147483648.0, 4294967295.0}) {
      double owner = owner();
      Capture closed = new Capture();
      enqueue("submit", closed, owner, 100.0, 6, 0.0, new byte[0], 0);
      drain();
      sync("close", owner, code);
      require(closed.values.size() == 1, "closed watcher must settle once");
      Object result = closed.values.get(0);
      require((Integer) field(result, "status") == 2, "local close must stay clean");
      require((Double) field(result, "handle") == code + 1, "close must preserve the full u32 code");
    }
  }

  public static void main(String[] args) throws Exception {
    T3QuicModuleTest test = new T3QuicModuleTest();
    if (Boolean.getBoolean("t3.test.process32")) {
      require(!(Boolean) test.sync("isAvailable"), "32-bit process must remain unavailable");
      require((Double) test.sync("createOwner") == 0, "32-bit process must never enter JNI");
      require(System.getProperty("t3.test.library.loaded") == null, "32-bit guard must run before JNI load");
      System.out.println("PASS: 32-bit guard without loading JNI");
      return;
    }
    require((Boolean) test.sync("isAvailable"), "test JNI must load");
    require("true".equals(System.getProperty("t3.test.library.loaded")), "JNI load marker must work");
    test.successfulOpenAfterCancel();
    test.queuedSubmitThenCancel();
    test.closeBeforeQueuedSubmit();
    test.closeCodeRange();
    require(((java.util.Map<?, ?>) field(test.module, "pending")).isEmpty(),
      "all wrapper completions must be released");
    System.out.println("PASS: late open, queued abort, queued close, duplicate callback, full u32 close codes");
  }

  private static final class Capture implements Promise {
    final List<Object> values = new ArrayList<>();

    @Override public void resolve(Object value) { values.add(value); }
    @Override public void reject(String code, String message, Throwable cause) {
      throw new AssertionError("unexpected Expo rejection: " + code + " " + message, cause);
    }
  }
}
