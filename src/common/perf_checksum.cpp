#include <map>
#include <vector>
#include <memory>
#include <string>
#include <iostream>

#include "HWCrc32c.h"
#include "SWCrc32c.h"
#include "IntelAsmCrc32c.h"

using namespace Hdfs::Internal;

static double seconds()
{
  timespec now;
  clock_gettime(CLOCK_REALTIME, &now);
  return now.tv_sec + now.tv_nsec / 1000000000.0;
}

static inline void benchmark(const std::shared_ptr<Checksum> & checksum, const void * buffer, int len, size_t retries, const std::string & tag)
{
    uint32_t sum_crc = 0;
    auto start = seconds();
    for (size_t i=0; i<retries; ++i)
    {
        checksum->reset();
        checksum->update(buffer, len);
        sum_crc += checksum->getValue();
    }
    auto duration = seconds() - start;
    std::cout << tag << "\t" << duration << "\t" << sum_crc << std::endl;
}

int main()
{
    std::map<std::string, std::shared_ptr<Checksum>> checksums = {
#if defined(__SSE4_2__) && defined(__LP64__)
        {"HWCrc32c", std::make_shared<HWCrc32c>()},
        {"IntelAsmCrc32c", std::make_shared<IntelAsmCrc32c>()},
#endif
        {"SWCrc32c", std::make_shared<SWCrc32c>()},
    };

    char * buffer = new char[512];
    for (size_t i = 0; i < 512; ++i)
        buffer[i] = (char)i;
    
    for (const auto & [tag, checksum] : checksums)
        benchmark(checksum, buffer, 512, 10000000UL, tag);

    return 0;
}
