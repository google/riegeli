package com.google.riegeli;

import java.io.IOException;

// JNI wrapper for riegeli record writer.
public class RecordWriter {

  public final static class Options {
    // Nothing is supported for now.
  }
  
  // Pointer to the C++ object.
  private long recordWriterPtr;

  // Options could be:
  // ```
  //   options ::= option? ("," option?)*
  //   option ::=
  //     "default" |
  //     "transpose" (":" ("true" | "false"))? |
  //     "uncompressed" |
  //     "brotli" (":" brotli_level)? |
  //     "zstd" (":" zstd_level)? |
  //     "snappy" |
  //     "window_log" ":" window_log |
  //     "chunk_size" ":" chunk_size |
  //     "bucket_fraction" ":" bucket_fraction |
  //     "pad_to_block_boundary" (":" ("true" | "false"))? |
  //     "parallelism" ":" parallelism
  //   brotli_level ::= integer in the range [0..11] (default 6)
  //   zstd_level ::= integer in the range [-131072..22] (default 3)
  //   window_log ::= "auto" or integer in the range [10..31]
  //   chunk_size ::= "auto" or positive integer expressed as real with
  //     optional suffix [BkKMGTPE]
  //   bucket_fraction ::= real in the range [0..1]
  //   parallelism ::= non-negative integer
  // ```
  public native void open(String filename, String options) throws IOException;

  public void writeRecord(String record) throws IOException {
    writeRecord(record.getBytes());
  }

  public native void writeRecord(byte[] record) throws IOException;

  // Flush the data into disk, more `writeRecord` can be called.
  public native void flush() throws IOException;
  
  public native void close() throws IOException;
}
