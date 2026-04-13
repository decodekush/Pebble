#ifndef MINI_LEVELDB_DB_FORMAT_H
#define MINI_LEVELDB_DB_FORMAT_H
#include <string>
#include <string_view>
#include <cstdint>

// --- Record Definitions ---
enum class OperationType : uint8_t // Physical status identifier: alternative to physical deletion
{
    kDeletion = 0x0,
    kValue = 0x1
};
struct ParsedRecord // Temporary record on stack
{
    std::string key;
    uint64_t seq;
    OperationType type;
    std::string value;
};
struct ParsedRecordView // Zero-copy memory observation window
{
    std::string_view key;
    uint64_t seq;
    OperationType type;
    std::string_view value;
};

// --- Search Definitions ---
enum class SearchStatus: uint8_t // Search Status
{
    kNotFound,
    kFoundValue,
    kFoundDeletion,
    FNotOpen
};
struct SearchResult // Internal search result
{
    SearchStatus status;
    std::string value;
};

#endif //MINI_LEVELDB_DB_FORMAT_H
