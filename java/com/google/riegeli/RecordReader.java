package com.google.riegeli;

import java.io.IOException;

// JNI wrapper for riegeli record reader.
public class RecordReader {

  public final static class Options {
    // Nothing is supported for now.
  }

  private long recordReaderPtr;

  public native void open(String filename) throws IOException;

  public native byte[] readRecord();

  public native void close() throws IOException;
}
