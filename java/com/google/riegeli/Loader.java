package com.google.riegeli;

import com.github.fmeum.rules_jni.RulesJni;

public class Loader {
  // Not sure whether it's worth the redirection to put JNI native lib loading logic in a single class.
  static {
    RulesJni.loadLibrary("riegeli_jni", RecordReader.class);
    RulesJni.loadLibrary("riegeli_jni", RecordWriter.class);
  }

  public final static RecordWriter newWriter() {
    return new RecordWriter();
  }

  public static RecordReader newReader() {
    return new RecordReader();
  }
}
