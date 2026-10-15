#!/usr/bin/env python3
"""Run the compiled Android wrapper with installed Expo classes and a controlled JNI boundary.

Pass the compiled module and its resolved dependency JARs as one classpath argument.
This checks wrapper interleavings on a Linux host JVM, not an Android/React runtime.
"""

import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import zipfile


def run(*args):
    subprocess.run(args, check=True)


def main():
    if len(sys.argv) != 2:
        raise SystemExit("usage: run-wrapper-tests.py <compiled-module-and-Expo-classpath>")
    source = Path(__file__).resolve().parent
    java_home = Path(os.environ["JAVA_HOME"]).resolve()
    scratch = Path.home() / "codex-tmp"
    scratch.mkdir(exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="t3-quic-wrapper-", dir=scratch) as folder:
        work = Path(folder)
        stubs = {
            "android/os/Build.java": """package android.os;
public final class Build { public static final String[] SUPPORTED_ABIS = {"x86_64"}; }
""",
            "android/os/Process.java": """package android.os;
public final class Process {
  public static boolean is64Bit() { return !Boolean.getBoolean("t3.test.process32"); }
}
""",
            "androidx/tracing/Trace.java": """package androidx.tracing;
public final class Trace {
  public static void beginSection(String name) {}
  public static void endSection() {}
}
""",
        }
        for name, contents in stubs.items():
            target = work / name
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(contents)
        dependencies = []
        for index, dependency in enumerate(sys.argv[1].split(os.pathsep)):
            if dependency.endswith(".aar"):
                target = work / f"dependency-{index}.jar"
                with zipfile.ZipFile(dependency) as archive:
                    target.write_bytes(archive.read("classes.jar"))
                dependencies.append(str(target))
            else:
                dependencies.append(dependency)
        dependencies = os.pathsep.join(dependencies)
        run(
            str(java_home / "bin/javac"), "-cp", dependencies, "-d", str(work),
            *(str(work / name) for name in stubs),
            str(source / "java/expo/modules/t3quic/T3QuicModuleTest.java"),
        )
        run(
            shutil.which("cc"), "-shared", "-fPIC", "-Wall", "-Wextra", "-Werror",
            "-Wno-unused-parameter", "-I" + str(java_home / "include"),
            "-I" + str(java_home / "include/linux"), str(source / "native-fixture.c"),
            "-o", str(work / "libt3_quic.so"),
        )
        classpath = str(work) + os.pathsep + dependencies
        run(
            str(java_home / "bin/java"), "-Dt3.test.process32=true",
            "-Djava.library.path=" + str(work), "-cp", classpath,
            "expo.modules.t3quic.T3QuicModuleTest",
        )
        run(
            str(java_home / "bin/java"), "-Djava.library.path=" + str(work),
            "-cp", classpath, "expo.modules.t3quic.T3QuicModuleTest",
        )


if __name__ == "__main__":
    main()
