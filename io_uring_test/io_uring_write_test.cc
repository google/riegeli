#include <string>

#include "riegeli/bytes/fd_io_uring_writer.h"
#include "riegeli/records/record_writer.h"
#include "riegeli/bytes/fd_reader.h"
#include "riegeli/records/record_reader.h"

#include "gtest/gtest.h"

namespace iouringtest {
using WritePtr = std::unique_ptr<riegeli::RecordWriter<riegeli::FdIoUringWriter<>>>;
using ReadPtr = std::unique_ptr<riegeli::RecordReader<riegeli::FdReader<>>>;

void SyncFd(WritePtr& writer, const std::string &file) {
    riegeli::FdIoUringOptions fd_io_uring_options;
    fd_io_uring_options.set_async(false);
    fd_io_uring_options.set_fd_register(true);

    riegeli::FdIoUringWriterBase::Options fd_w_options;
    fd_w_options.set_io_uring_option(fd_io_uring_options);
    
    riegeli::RecordWriterBase::Options w_options;

    riegeli::FdIoUringWriter<> fd_writer(file, O_WRONLY | O_CREAT | O_TRUNC,
                                 fd_w_options);

    writer = std::make_unique<riegeli::RecordWriter<riegeli::FdIoUringWriter<>>>(
    std::move(fd_writer), std::move(w_options));
}

void Sync(WritePtr& writer, const std::string &file) {
    riegeli::FdIoUringOptions fd_io_uring_options;
    fd_io_uring_options.set_async(false);
    fd_io_uring_options.set_fd_register(false);

    riegeli::FdIoUringWriterBase::Options fd_w_options;
    fd_w_options.set_io_uring_option(fd_io_uring_options);
    
    riegeli::RecordWriterBase::Options w_options;

    riegeli::FdIoUringWriter<> fd_writer(file, O_WRONLY | O_CREAT | O_TRUNC,
                                 fd_w_options);

    writer = std::make_unique<riegeli::RecordWriter<riegeli::FdIoUringWriter<>>>(
    std::move(fd_writer), std::move(w_options));
}

void WriteData(WritePtr &writer) {
    for(int i = 1; i <= 100000000; ++i) {
        std::string temp = std::to_string(i);
        writer -> WriteRecord(temp);
    }
    writer -> Close();
}

void CheckData(const std::string &file) {
    riegeli::RecordReaderBase::Options r_options;
    riegeli::FdReaderBase::Options fd_r_options;
    riegeli::FdReader<> fd_reader(file, O_RDONLY,
                                   fd_r_options);
    ReadPtr reader = std::make_unique<riegeli::RecordReader<riegeli::FdReader<>>>(
    std::move(fd_reader), std::move(r_options));
    
    std::string record;
    int num = 1;
    while(reader -> ReadRecord(record)) {
        EXPECT_EQ(record, std::to_string(num));
        ++num;
    }
    EXPECT_EQ(num, 100000001);
    reader -> Close();
}

void WriteLargeData(WritePtr &writer) {
    for(int i = 1; i <= 10000; ++i) {
        std::string element = std::to_string(i);
        std::string temp = element;
        for(int j = 0; j < 100000; ++j) {
            temp += element;
        }
        writer -> WriteRecord(temp);
    }
    writer -> Close();
}

void CheckLargeData(const std::string &file) {
    riegeli::RecordReaderBase::Options r_options;
    riegeli::FdReaderBase::Options fd_r_options;
    riegeli::FdReader<> fd_reader(file, O_RDONLY,
                                   fd_r_options);
    ReadPtr reader = std::make_unique<riegeli::RecordReader<riegeli::FdReader<>>>(
    std::move(fd_reader), std::move(r_options));
    
    std::string record;
    int num = 1;
    while(reader -> ReadRecord(record)) {
        std::string element = std::to_string(num);
        std::string temp = element;
        for(int j = 0; j < 100000; ++j) {
            temp += element;
        }
        EXPECT_EQ(record, temp);
        ++num;
    }
    EXPECT_EQ(num, 10001);
    reader -> Close();
}

TEST(IoUringTest, SynWrite) {
    std::string file = "/home/werider/riegeli/io_uring_test/syn_test_file";
    WritePtr writer;
    Sync(writer, file);

    WriteData(writer);
    CheckData(file);    
}

TEST(IoUringTest, SynFdWrite) {
    std::string file = "/home/werider/riegeli/io_uring_test/syn_test_file";
    WritePtr writer;
    SyncFd(writer, file);

    WriteData(writer);
    CheckData(file);    
}


TEST(IoUringTest, SynWriteLargeData) {
    std::string file = "/home/werider/riegeli/io_uring_test/syn_test_file";
    WritePtr writer;
    Sync(writer, file);

    WriteLargeData(writer);
    CheckLargeData(file);  
}

TEST(IoUringTest, SynFdWriteLargeData) {
    std::string file = "/home/werider/riegeli/io_uring_test/syn_test_file";
    WritePtr writer;
    SyncFd(writer, file);

    WriteLargeData(writer);
    CheckLargeData(file);    
}

}