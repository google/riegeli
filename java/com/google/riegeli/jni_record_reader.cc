#include "com_google_riegeli_RecordReader.h"

#include "riegeli/bytes/fd_reader.h"
#include "riegeli/records/record_reader.h"

#ifdef __cplusplus
extern "C" {
#endif
/*
 * Class:     com_google_riegeli_RecordReader
 * Method:    open
 * Signature: (Ljava/lang/String;)V
 */
using ReaderType = riegeli::RecordReader<riegeli::FdReader<>>;
  
JNIEXPORT void JNICALL Java_com_google_riegeli_RecordReader_open(JNIEnv* env, jobject reader, jstring filename) {
  /* Obtain a C-copy of the Java string */ 
  const char* fname = env->GetStringUTFChars(filename, nullptr); 

  /* Create the recorder */
  riegeli::RecordReaderBase::Options record_reader_options;
  auto* record_reader = new ReaderType(
      std::forward_as_tuple(fname, O_RDONLY),
      record_reader_options);
  
  env->ReleaseStringUTFChars(filename, fname);   

  /* Get the Field ID of the instance variables "recordReaderPtr" */
  jfieldID fid = env->GetFieldID(env->GetObjectClass(reader), "recordReaderPtr", "J");

  // Save the pointer as member.
  env->SetLongField(reader, fid, reinterpret_cast<jlong>(record_reader));
}

namespace {
ReaderType* getRecordReader(JNIEnv* env, jobject reader) {
  jfieldID fid = env->GetFieldID(env->GetObjectClass(reader), "recordReaderPtr", "J");
  jlong ptr = env->GetLongField(reader, fid);
  return reinterpret_cast<ReaderType*>(ptr);
}
}

/*
 * Class:     com_google_riegeli_RecordReader
 * Method:    readRecord
 * Signature: ()[B
 */
JNIEXPORT jbyteArray JNICALL Java_com_google_riegeli_RecordReader_readRecord(JNIEnv* env, jobject obj) {
  auto* record_reader = getRecordReader(env, obj);
  if (!record_reader) {
    return nullptr;
  }
  std::string record;
  if (record_reader->ReadRecord(record)) {
    jbyteArray ret = env->NewByteArray(record.size());
    env->SetByteArrayRegion(ret, 0, record.size(), reinterpret_cast<const jbyte*>(record.data()));
    return ret;
  } else {
    return nullptr;
  }
}

/*
 * Class:     com_google_riegeli_RecordReader
 * Method:    close
 * Signature: ()V
 */
JNIEXPORT void JNICALL Java_com_google_riegeli_RecordReader_close(JNIEnv* env, jobject reader) {
  auto* record_reader = getRecordReader(env, reader);
  if (record_reader) {
    record_reader->Close();
    delete record_reader;

    jfieldID fid = env->GetFieldID(env->GetObjectClass(reader), "recordReaderPtr", "J");
    env->SetLongField(reader, fid, 0L);
  }
}

#ifdef __cplusplus
}
#endif

