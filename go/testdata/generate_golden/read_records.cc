// Reads records from a Riegeli file and prints them, one per line,
// with a length prefix. Used for cross-language compatibility testing.
//
// Output format: one line per record: "<hex_length> <hex_data>\n"
// At the end: "EOF <num_records>\n"
//
// Usage: read_records <input_file>

#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <string>

#include "absl/strings/string_view.h"
#include "riegeli/bytes/fd_reader.h"
#include "riegeli/records/record_reader.h"

int main(int argc, char* argv[]) {
  if (argc != 2) {
    std::cerr << "Usage: " << argv[0] << " <input_file>" << std::endl;
    return 1;
  }

  riegeli::RecordReader<riegeli::FdReader<>> reader{
      riegeli::FdReader<>(argv[1])};

  int count = 0;
  absl::string_view record;
  while (reader.ReadRecord(record)) {
    // Print length in decimal, then space, then record bytes as hex.
    std::cout << record.size();
    for (char c : record) {
      std::cout << ' ' << std::hex << std::setw(2) << std::setfill('0')
                << (static_cast<unsigned int>(static_cast<unsigned char>(c)));
    }
    std::cout << std::dec << std::endl;
    count++;
  }
  if (!reader.Close()) {
    std::cerr << "Error reading: " << reader.status() << std::endl;
    return 1;
  }
  std::cout << "EOF " << count << std::endl;
  return 0;
}
