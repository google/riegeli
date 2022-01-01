#include "com_google_riegeli_RecordWriter.h"

#include "riegeli/bytes/fd_writer.h"
#include "riegeli/records/record_writer.h"

#ifdef __cplusplus
extern "C" {
#endif

using WriterType = riegeli::RecordWriter<riegeli::FdWriter<>>;
/*
 * Class:     com_google_riegeli_RecordWriter
 * Method:    open
 * Signature: (Ljava/lang/String;)V
 */
JNIEXPORT void JNICALL Java_com_google_riegeli_RecordWriter_open(JNIEnv* env, jobject writer, jstring filename, jstring options) {
  // Prepare the options
  riegeli::RecordWriterBase::Options record_writer_options;
  {
    const char* options_str = env->GetStringUTFChars(options, nullptr);
    const auto status = record_writer_options.FromString(options_str);
    env->ReleaseStringUTFChars(options, options_str);   
  }

  // Create the writer
  const char* fname = env->GetStringUTFChars(filename, nullptr); 
  auto* record_writer = new WriterType(
      std::forward_as_tuple(fname, O_WRONLY | O_CREAT | O_TRUNC),
      record_writer_options);
  env->ReleaseStringUTFChars(filename, fname);   

  /* Get the Field ID of the instance variables "recordReaderPtr" */
  jfieldID fid = env->GetFieldID(env->GetObjectClass(writer), "recordWriterPtr", "J");

  // Save the pointer as member.
  env->SetLongField(writer, fid, reinterpret_cast<jlong>(record_writer));
}

namespace {
WriterType* getRecordWriter(JNIEnv* env, jobject writer) {
  jfieldID fid = env->GetFieldID(env->GetObjectClass(writer), "recordWriterPtr", "J");
  jlong ptr = env->GetLongField(writer, fid);
  return reinterpret_cast<WriterType*>(ptr);
}

void throwException(JNIEnv* env, const char* exceptionClass, const char* message) {
  jclass Exception = env->FindClass(exceptionClass);
  env->ThrowNew(Exception, message); // Error Message
}

void throwIllegalStateException(JNIEnv* env, const char* message) {
  throwException(env, "java/lang/IllegalStateException", message);
}

void throwIOException(JNIEnv* env, const char* message) {
  throwException(env, "java/io/IOException", message);
}

}

JNIEXPORT void JNICALL Java_com_google_riegeli_RecordWriter_writeRecord(
    JNIEnv* env, jobject writer, jbyteArray record) {
  auto* native_writer = getRecordWriter(env, writer);
  if (!native_writer) {
    throwIllegalStateException(env, "open should have been called");
    return;
  }
  // TODO: throw a runtime exception if `open` method has not been
  // called successfully.
  const jint size = env->GetArrayLength(record);
  jbyte* data = env->GetByteArrayElements(record, 0);
  // TODO: check the return value and status
  bool ret = native_writer->WriteRecord(absl::string_view(reinterpret_cast<const char*>(data), size));
  env->ReleaseByteArrayElements(record, data, 0);
  if (!ret) {
    throwIOException(env, "Fail to write record");
  }
}

JNIEXPORT void JNICALL Java_com_google_riegeli_RecordWriter_flush(JNIEnv* env, jobject writer) {
  auto* record_writer = getRecordWriter(env, writer);
  if (record_writer) {
    record_writer->Flush();
  } else {
    throwIllegalStateException(env, "open should have been called");
  }
}

JNIEXPORT void JNICALL Java_com_google_riegeli_RecordWriter_close(JNIEnv* env, jobject writer) {
  auto* record_writer = getRecordWriter(env, writer);
  if (record_writer) {
    record_writer->Close();
    delete record_writer;

    jfieldID fid = env->GetFieldID(env->GetObjectClass(writer), "recordWriterPtr", "J");
    env->SetLongField(writer, fid, 0L);
  } else {
    throwIllegalStateException(env, "open should have been called");
  }
}
 

#ifdef __cplusplus
}
#endif

