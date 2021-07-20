#include <string>

#include "riegeli/bytes/fd_io_uring_writer.h"
#include "riegeli/records/record_writer.h"
#include "riegeli/bytes/fd_reader.h"
#include "riegeli/records/record_reader.h"

using WritePtr = std::unique_ptr<riegeli::RecordWriter<riegeli::FdIoUringWriter<>>>;
using ReadPtr = std::unique_ptr<riegeli::RecordReader<riegeli::FdReader<>>>;

void CheckData(const std::string &file) {
    riegeli::RecordReaderBase::Options r_options;
    riegeli::FdReaderBase::Options fd_r_options;
    riegeli::FdReader<> fd_reader(file, O_RDONLY,
                                   fd_r_options);
    ReadPtr reader = std::make_unique<riegeli::RecordReader<riegeli::FdReader<>>>(
    std::move(fd_reader), std::move(r_options));

    std::string record;
    int num = 1;
    int result = 0;
    while(reader -> ReadRecord(record)) {
        if(record == std::to_string(num)) {
            ++result;
        }
        ++num;
    }
    std::cout << result << std::endl;
    reader -> Close();
}


int main() {
    std::string file = "/home/werider/riegeli/io_uring_test/syn_test_file";
    riegeli::FdIoUringOptions fd_io_uring_options;
    fd_io_uring_options.set_async(true);
    fd_io_uring_options.set_fd_register(false);

    riegeli::FdIoUringWriterBase::Options fd_w_options;
    fd_w_options.set_io_uring_option(fd_io_uring_options);
    
    riegeli::RecordWriterBase::Options w_options;

    riegeli::FdIoUringWriter<> fd_writer(file, O_WRONLY | O_CREAT | O_TRUNC,
                                 fd_w_options);
    WritePtr writer = std::make_unique<riegeli::RecordWriter<riegeli::FdIoUringWriter<>>>(
    std::move(fd_writer), std::move(w_options));
    for(int i = 1; i <= 100000; ++i) {
        std::string temp = std::to_string(i);
        writer -> WriteRecord(temp);
    }
    writer -> Close();
    CheckData(file);    
    return 0;
}