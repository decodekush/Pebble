#ifndef MINI_LEVELDB_MINI_KV_STORE_H
#define MINI_LEVELDB_MINI_KV_STORE_H

#include <atomic>
#include <condition_variable>
#include <shared_mutex>
#include <fstream>
#include <memory>
#include <deque>
#include <unordered_map>
#include <cstring> // Provides memcpy for subsequent memory byte stream decoding
#include <list>
#include <mutex>
#include <thread>
#include <exception>

#include "mini_leveldb/mem_table.h"
#include "mini_leveldb/compaction.h"

enum class CompactionType {
    kLeveled,
    kSizeTiered
};

// Database Interface
// Physical carrier: standard data unit extracted from disk
class mini_kv_store
{
    friend class CompactionStrategy;
    friend class LeveledCompaction;
    friend class SizeTieredCompaction;
public:
    explicit mini_kv_store(CompactionType compaction_type = CompactionType::kLeveled);
    ~mini_kv_store();

    // Prevent copying and moving
    mini_kv_store(const mini_kv_store&) = delete;
    mini_kv_store& operator=(const mini_kv_store&) = delete;
    mini_kv_store(mini_kv_store&&) = delete;
    mini_kv_store& operator=(mini_kv_store&&) = delete;

    void close();
    [[nodiscard]] std::string get(std::string_view) const;
    void put(std::string_view, std::string_view);
    void erase(std::string_view);
private:
    mutable std::shared_mutex lifecycle_mutex_;
    bool closed_ = false;
    // --- Active In-Memory Skip List ---
    mutable std::shared_mutex rw_mutex; // In-memory skip list read-write lock

    std::shared_ptr<mem_table> data_ = std::make_shared<mem_table>(); // Active in-memory skip list
    std::atomic<uint64_t> current_seq_ = 0; // Maintains globally unique sequence number
    size_t memtable_size_threshold_ = 4096;

    // --- Serial Log Append ---
    std::ofstream wal_file_;
    mutable std::mutex wal_mutex_;
    void append_to_wal(OperationType, uint64_t, std::string_view, std::string_view);

    // --- Cache: LRU Block Retrieval ---
    struct BlockPos // Unique 3D coordinates of the data block in the multi-dimensional matrix
    {
        uint64_t sst_id;
        uint64_t offset;
        bool operator==(const BlockPos& other) const // Support hash collision
        {
            return sst_id == other.sst_id && offset == other.offset;
        }
    };
    struct BlockPosHash // Folds 2D physical coordinates into a 1D hash delimiter
    {
        size_t operator()(const BlockPos& k) const
        {
            return std::hash<uint64_t>()(k.sst_id) ^ (std::hash<uint64_t>()(k.offset) << 1); // Uses bit shifting for hash obfuscation, preventing low-order collisions
        }
    };
    struct lru_cache
    {
        const size_t capacity_ = 1024; // Physical limit: caches approx 4 MB of data blocks
        mutable std::mutex mtx_; // Absolute exclusive lock, intercepts concurrent pointer redirection across multiple threads
        struct block_elem
        {
            BlockPos pos;
            std::vector<std::byte> data;
        };
        mutable std::list<std::shared_ptr<block_elem>> lru_queue_; // Physical timeline: doubly linked list, head is MRU (newest), tail is LRU (oldest)
        mutable std::unordered_map<BlockPos, decltype(lru_queue_)::iterator, BlockPosHash> block_map_; // Mapping of block coordinates to queue nodes
    } search_cache_;

    std::shared_ptr<const std::vector<std::byte>> fetch_block(uint64_t, uint64_t, uint64_t) const; // Disk probe upgrade: instead of reading word by word, violently extracts the entire data block
    SearchResult search_in_sstable(std::string_view key, uint64_t sst_id) const;

    // --- Single Background Flush Thread Scheduling ---
    mutable std::shared_mutex bg_mutex_; // Flush queue lock

    std::deque<std::shared_ptr<mem_table>> bg_queue_; // Flush task queue
    std::condition_variable_any bg_cv_; // Flush task condition variable / waker
    void background_compaction_routine(); // Flush scheduling routine
    std::thread bg_thread_; // Flush thread
    std::atomic<bool> stop_bg_thread_{false}; // Inter-thread communication - shutdown flag
    std::exception_ptr bg_error_;
    void flush_imm_to_sstable(const std::shared_ptr<mem_table>&); // Flush routine
    class sst_scanner // SST record reader
    {
        std::ifstream file_;
        uint64_t index_offset_;
    public:
        explicit sst_scanner(std::string_view);

        [[nodiscard]] bool has_next();
        [[nodiscard]] ParsedRecord next();
    };
    std::unique_ptr<CompactionStrategy> compaction_strategy_;
    CompactionType compaction_type_;

    // --- Multi-level File Matrix (Metadata) ---
    mutable std::shared_mutex manifest_mutex_; // Metadata manifest lock

    std::vector<std::vector<uint64_t>> levels_; // Core physical matrix: index represents level (0 is L0, 1 is L1, up to L6)
    std::atomic<uint64_t> next_sst_id_ {1}; // Globally unique physical file addressing
    std::string generate_sst_filename(uint64_t id) const; // SST filename generator
    void save_manifest() const; // Metadata persistence
    void load_manifest_and_rebuild_cache(); //Metadata reading and cache reconstruction

    // --- Cache: Sparse Key-Value Index, Bloom Filter ---
    mutable std::shared_mutex cache_mutex_; // cache_mutex_ Cache lock

    struct index_record
    {
        std::string key;
        uint64_t offset;
    };
    mutable std::unordered_map<uint64_t, std::vector<index_record>> index_cache_; // Sparse index storage structure: <SST_ID, [<key, offset>]>

    mutable std::unordered_map<uint64_t, std::vector<uint8_t>> bloom_cache_; // Bloom filter cache storage structure: <SST_ID, binary bitmap>
    static uint32_t bloom_hash(std::string_view, uint32_t); // Underlying hash generator
    std::vector<uint8_t> generate_bloom_filter(const std::vector<std::string>&) const; // Generates fixed-length bitmap from key list
    bool may_exist_in_sst(uint64_t, std::string_view) const;  // Probe pre-interceptor
};

#endif //MINI_LEVELDB_MINI_KV_STORE_H
