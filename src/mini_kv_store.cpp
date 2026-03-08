#include <mutex>
#include <filesystem>
#include <complex>
#include <queue>
#include <ranges>
#include <algorithm>
#include <stdexcept>
#include <limits>
#include <array>
#ifdef _WIN32
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>
#endif

#include "mini_leveldb/mini_kv_store.h"
#include "mini_leveldb/mem_table.h"
#include "mini_leveldb/coding.h"

mini_kv_store::mini_kv_store(CompactionType compaction_type): levels_(7), compaction_type_(compaction_type)
{
    if (compaction_type == CompactionType::kLeveled) {
        compaction_strategy_ = std::make_unique<LeveledCompaction>();
    } else {
        compaction_strategy_ = std::make_unique<SizeTieredCompaction>();
    }

    load_manifest_and_rebuild_cache();
    const std::string wal_path = "wal.log";
    if (std::filesystem::exists(wal_path))
    {
        std::ifstream wal_in(wal_path, std::ios::binary);
        if (!wal_in) throw std::runtime_error("Cannot open WAL for recovery");
        const uint64_t wal_size = std::filesystem::file_size(wal_path);
        uint64_t valid_size = 0;
        while (valid_size < wal_size)
        {
            const uint64_t remaining = wal_size - valid_size;
            constexpr uint64_t fixed_bytes = sizeof(uint8_t) + sizeof(uint64_t) + sizeof(uint32_t) + sizeof(uint32_t);
            if (remaining < fixed_bytes) break;
            uint8_t type_raw = 0;
            uint64_t seq = 0;
            uint32_t key_size = 0, value_size = 0;
            if (!coding::read_raw(wal_in, type_raw) ||
                !coding::read_raw(wal_in, seq) ||
                !coding::read_raw(wal_in, key_size)) break;
            uint64_t consumed = sizeof(type_raw) + sizeof(seq) + sizeof(key_size);
            if (key_size > remaining - consumed - sizeof(value_size)) break;
            std::string key = coding::read_slice(wal_in, key_size);
            if (!wal_in || !coding::read_raw(wal_in, value_size)) break;
            consumed += key_size + sizeof(value_size);
            if (value_size > remaining - consumed) break;
            std::string value = coding::read_slice(wal_in, value_size);
            if (!wal_in || (type_raw != static_cast<uint8_t>(OperationType::kValue) &&
                            type_raw != static_cast<uint8_t>(OperationType::kDeletion))) break;
            if (type_raw == static_cast<uint8_t>(OperationType::kValue))
                data_->insert_record(key, seq, value);
            else
                data_->insert_tombstone(key, seq);
            if (seq >= current_seq_.load(std::memory_order_relaxed))
                current_seq_.store(seq + 1, std::memory_order_relaxed);
            valid_size += consumed + value_size;
        }
        wal_in.close();
        if (valid_size != wal_size) std::filesystem::resize_file(wal_path, valid_size);
    }
    wal_file_.open("wal.log", std::ios::app | std::ios::binary);
    if (!wal_file_.is_open())
    {
        throw std::runtime_error("Fatal: Unable to establish WAL physical pipe");
    }

    bg_thread_  = std::thread(&mini_kv_store::background_compaction_routine, this);
}

mini_kv_store::~mini_kv_store()
{
    try { close(); } catch (...) { /* WAL remains available for recovery. */ }
}

void mini_kv_store::close()
{
    std::unique_lock lifecycle_lock(lifecycle_mutex_);
    if (closed_) return;
    if (data_->approximate_memory_usage() != 0)
    {
        std::lock_guard lock(bg_mutex_);
        bg_queue_.push_back(data_);
        data_.reset();
    }
    closed_ = true;
    {
        std::lock_guard lock(bg_mutex_);
        stop_bg_thread_ = true;
    }
    bg_cv_.notify_all();
    if (bg_thread_.joinable()) bg_thread_.join();
    {
        std::lock_guard wal_lock(wal_mutex_);
        if (wal_file_.is_open())
        {
            wal_file_.flush();
            if (!wal_file_) throw std::runtime_error("Cannot flush WAL during close");
            wal_file_.close();
        }
    }
    if (bg_error_) std::rethrow_exception(bg_error_);
    std::ofstream clear_wal("wal.log", std::ios::binary | std::ios::trunc);
    if (!clear_wal) throw std::runtime_error("Cannot truncate committed WAL");
    clear_wal.close();
    if (!clear_wal) throw std::runtime_error("Cannot close committed WAL");
}

std::string mini_kv_store::get(const std::string_view key) const
{
    std::shared_lock lifecycle_lock(lifecycle_mutex_);
    if (closed_) throw std::runtime_error("Store is closed");
    // Penetration level 1: Observe highest frequency Active MemTable
    {
        // Acquire shared lock during construction, release automatically during destruction
        std::shared_lock lock(rw_mutex);
        auto [status, value] = data_->get(key);
        if (status == SearchStatus::kFoundValue) return value;
        if (status == SearchStatus::kFoundDeletion) return "";
    } // Release rw_mutex, avoid long blocking of external writes

    // Penetration level 2: Observe Immutable MemTables in Queue in physical transit
    {
        std::shared_lock lock(bg_mutex_); // Lock background queue bus
        // Reverse iterate deque (scan from newest frozen skip list to older ones)
        for (const auto& it : std::ranges::reverse_view(bg_queue_))
        {
            auto [status, value] = it->get(key);
            if (status == SearchStatus::kFoundValue) return value;
            if (status == SearchStatus::kFoundDeletion) return "";
        }
    } // Release bg_mutex_

    // Penetration level 3: Multi-dimensional physical matrix cascading search
    // Keep source files alive until the lookup finishes; compaction unlinks them
    // only while holding the exclusive manifest lock.
    std::shared_lock manifest_lock(manifest_mutex_);
    const auto& current_matrix = levels_;

    // Absolute partial order 1: Level 0 has highest time weight, reverse iterate internal files
    for (const auto id : std::ranges::reverse_view(current_matrix[0]))
    {
        if (!may_exist_in_sst(id, key)) continue; // L0 can also apply bloom interception
        auto disk_result = search_in_sstable(key, id);
        if (disk_result.status == SearchStatus::FNotOpen) throw std::runtime_error("Cannot read referenced SST");
        if (disk_result.status == SearchStatus::kFoundValue) return disk_result.value;
        if (disk_result.status == SearchStatus::kFoundDeletion) return "";
    }

    // Absolute partial order 2: Level 1 to N, data is mutually exclusive and globally ordered (for Leveled)
    for (size_t lvl = 1; lvl < current_matrix.size(); ++lvl)
    {
        const auto& current_level_files = current_matrix[lvl];
        if (current_level_files.empty()) continue;

        if (compaction_type_ == CompactionType::kSizeTiered) {
            // For SizeTiered, files in the same level can overlap and are ordered by creation time
            for (const auto id : std::ranges::reverse_view(current_level_files)) {
                if (!may_exist_in_sst(id, key)) continue;
                auto disk_result = search_in_sstable(key, id);
                if (disk_result.status == SearchStatus::FNotOpen) throw std::runtime_error("Cannot read referenced SST");
                if (disk_result.status == SearchStatus::kFoundValue) return disk_result.value;
                if (disk_result.status == SearchStatus::kFoundDeletion) return "";
            }
        } else {
            uint64_t target_sst_id = 0;
            bool found_file = false;

            {
                std::shared_lock cache_lock(cache_mutex_);
                // Find first SSTable where min Key is strictly greater than target Key
                auto it = std::upper_bound(current_level_files.begin(), current_level_files.end(), key,
                    [this](const std::string_view k, const uint64_t id)
                    {
                        if (index_cache_.at(id).empty()) return false;
                        return k < index_cache_.at(id).front().key;
                    });
                // Step back one position, lock the only physical file that might contain the target data
                if (it != current_level_files.begin())
                {
                    target_sst_id = *(it - 1);
                    found_file = true;
                }
            }

            if (found_file)
            {
                if (!may_exist_in_sst(target_sst_id, key)) continue;

                auto disk_result = search_in_sstable(key, target_sst_id);
                if (disk_result.status == SearchStatus::FNotOpen) throw std::runtime_error("Cannot read referenced SST");
                if (disk_result.status == SearchStatus::kFoundValue) return disk_result.value;
                if (disk_result.status == SearchStatus::kFoundDeletion) return "";
            }
        }
    }

    return ""; // Absolute vacuum, data does not exist
}

void mini_kv_store::put(const std::string_view key, std::string_view value)
{
    std::shared_lock lifecycle_lock(lifecycle_mutex_);
    if (closed_) throw std::runtime_error("Store is closed");
    // Acquire exclusive lock during construction, execute physical clearing
    std::unique_lock lock(rw_mutex);

    // 1. Capacity probe interception
    if (data_->approximate_memory_usage() >= memtable_size_threshold_)
    {
        // 1. Push frozen skip list into asynchronous scheduling queue
        {
            std::lock_guard bg_lock(bg_mutex_);
            bg_queue_.push_back(std::move(data_));
            // 2. Reconstruct active skip list in place
            data_ = std::make_shared<mem_table>();
        }
        bg_cv_.notify_one(); // Send hardware-level wakeup pulse to background thread
    }

    const uint64_t seq = current_seq_.fetch_add(1, std::memory_order_relaxed) & 0x00FFFFFFFFFFFFFF;
    // Physical defense: flush to disk first
    append_to_wal(OperationType::kValue, seq, key, value);
    // Absolute exclusive state
    data_->insert_record(key, seq, value);
}

void mini_kv_store::erase(const std::string_view key)
{
    std::shared_lock lifecycle_lock(lifecycle_mutex_);
    if (closed_) throw std::runtime_error("Store is closed");
    // Acquire exclusive lock during construction, execute physical clearing
    std::unique_lock lock(rw_mutex);
    if (data_->approximate_memory_usage() >= memtable_size_threshold_)
    {
        auto replacement = std::make_shared<mem_table>();
        {
            std::lock_guard bg_lock(bg_mutex_);
            bg_queue_.push_back(data_);
            data_ = std::move(replacement);
        }
        bg_cv_.notify_one();
    }
    const uint64_t seq = current_seq_.fetch_add(1, std::memory_order_relaxed) & 0x00FFFFFFFFFFFFFF;
    // Physical defense: flush first, set deletion value to empty
    append_to_wal(OperationType::kDeletion, seq, key, "");
    // Absolute exclusive state
    data_->insert_tombstone(key, seq);
}

void mini_kv_store::append_to_wal(OperationType type, uint64_t seq, const std::string_view key, const std::string_view value)
{
    std::lock_guard wal_lock(wal_mutex_);

    const auto t = static_cast<uint8_t>(type);
    coding::write_raw(wal_file_, t);

    coding::write_raw(wal_file_, seq);

    const uint32_t key_size = key.size();
    coding::write_raw(wal_file_, key_size);
    coding::write_slice(wal_file_, key);

    const uint32_t value_size = value.size();
    coding::write_raw(wal_file_, value_size);
    coding::write_slice(wal_file_, value);

    wal_file_.flush();
    if (!wal_file_) throw std::runtime_error("WAL append failed");
}

void mini_kv_store::background_compaction_routine()
{
    while (true)
    {
        std::shared_ptr<mem_table> imm_to_flush;
        {
            // Acquire system scheduling lock, probe queue state
            std::unique_lock lock(bg_mutex_);
            bg_cv_.wait(lock, [this]
            {
                return stop_bg_thread_ || !bg_queue_.empty();
            });

            // If shutdown signal is received and queue is physically drained, terminate current execution flow
            if (stop_bg_thread_ && bg_queue_.empty())
                return;

            // Physically detach the frozen list at the head of the queue
            imm_to_flush = bg_queue_.front(); // Copy shared_ptr, absolutely don't pop queue. At this time, foreground Get can still safely iterate bg_queue to read data.
        } // Leave scope, instantly release bg_mutex_

        // Absolute compute decoupling: execute slow disk ops here, away from main thread locks, frictionless flush
        try
        {
            flush_imm_to_sstable(imm_to_flush);
        }
        catch (...)
        {
            std::lock_guard lock(bg_mutex_);
            bg_error_ = std::current_exception();
            return;
        }

        // Disk burning complete, data absolutely visible in SSTable. Now we can acquire lock to physically detach skip list from memory queue
        {
            std::unique_lock lock(bg_mutex_);
            bg_queue_.pop_front();
        }

        try { compaction_strategy_->compact(this); }
        catch (...)
        {
            std::lock_guard lock(bg_mutex_);
            bg_error_ = std::current_exception();
            return;
        }
    }
}

void mini_kv_store::flush_imm_to_sstable(const std::shared_ptr<mem_table>& imm_to_flush)
{
    // 1. Physical organization: extract currently available sequence number and atomically advance
    uint64_t sst_id = next_sst_id_.fetch_add(1, std::memory_order_relaxed); //?
    std::string sst_path = generate_sst_filename(sst_id);

    std::ofstream sst_file(sst_path, std::ios::binary | std::ios::trunc);
    if (!sst_file.is_open()) throw std::runtime_error("Cannot open SST for flush");

    std::vector<index_record> sparse_index;

    uint64_t current_offset = 0;
    uint64_t last_block_offset = 0;
    std::string previous_key;
    bool have_previous_key = false;

    std::vector<std::string> all_keys_for_bloom;

    // 1. Sequentially etch Data Block
    for (auto it = imm_to_flush -> begin(); it != imm_to_flush->end(); ++it)
    {
        auto view = *it;
        all_keys_for_bloom.emplace_back(view.key);

        auto raw_payload = it.raw_payload();

        if (constexpr uint64_t PAGE_SIZE = 4096;
            (current_offset - last_block_offset >= PAGE_SIZE || current_offset == 0) &&
            (!have_previous_key || view.key != previous_key))
        {
            sparse_index.emplace_back(std::string(view.key), current_offset);
            last_block_offset = current_offset;
        }

        coding::write_buffer(sst_file, raw_payload);
        current_offset += raw_payload.size();
        previous_key = view.key;
        have_previous_key = true;
    }

    // 2. Sequentially etch Index Block
    uint64_t index_block_offset = current_offset;
    for (const auto& [idx_key, idx_offset]: sparse_index)
    {
        uint32_t k_size = idx_key.size();
        coding::write_raw(sst_file, k_size);
        coding::write_slice(sst_file, idx_key);
        coding::write_raw(sst_file, idx_offset);
    }

    // 3. Etch Bloom Filter Block
    uint64_t bloom_block_offset = sst_file.tellp();
    std::vector<uint8_t> bloom_data = generate_bloom_filter(all_keys_for_bloom);
    coding::write_buffer(sst_file, std::span<const uint8_t>{bloom_data});

    // 4. Etch upgraded Footer (16Bytes)
    // Contains: 8-byte index block offset + 8-byte bloom block offset
    coding::write_raw(sst_file, index_block_offset);
    coding::write_raw(sst_file, bloom_block_offset);

    sst_file.flush();
    if (!sst_file) throw std::runtime_error("Cannot write SST during flush");
    sst_file.close();
    if (!sst_file) throw std::runtime_error("Cannot close SST after flush");

    // Topological update: insert newly effective bitmap directly into memory cache network
    {
        std::unique_lock cache_lock(cache_mutex_);
        bloom_cache_[sst_id] = std::move(bloom_data);
        index_cache_[sst_id] = std::move(sparse_index);
    }

    // Topological registration: register completed physical file to Level 0 manifest
    {
        std::unique_lock manifest_lock(manifest_mutex_);
        levels_[0].push_back(sst_id);
        try { save_manifest(); }
        catch (...) { levels_[0].pop_back(); throw; }
    }
}

mini_kv_store::sst_scanner::sst_scanner(const std::string_view path)
{
    file_.open(path.data(), std::ios::binary);
    if (file_.is_open())
    {
        // Read 8-byte Footer, lock physical boundary of data area
        // Physical repair: move probe to last 16 bytes (absolute start of Footer)
        file_.seekg(-static_cast<std::streamoff>(sizeof(uint64_t) * 2), std::ios::end);
        // Extract first 8 bytes index_offset as data block boundary, discard last 8 bytes bloom_offset
        coding::read_raw(file_, index_offset_);
        // Return read probe to absolute start of file, prepare to sequentially extract data blocks
        file_.seekg(0, std::ios::beg);
    }
    else
    {
        index_offset_ = 0;
    }
}

bool mini_kv_store::sst_scanner::has_next()
{
    if (!file_.is_open()) return false;
    const auto position = file_.tellg();
    return position != std::streampos(-1) &&
           static_cast<uint64_t>(static_cast<std::streamoff>(position)) < index_offset_;
}

ParsedRecord mini_kv_store::sst_scanner::next()
{
    ParsedRecord record;
    uint32_t internal_key_size;
    coding::read_raw(file_, internal_key_size);

    const uint32_t real_key_size = internal_key_size - sizeof(uint64_t);
    record.key = coding::read_slice(file_, real_key_size);

    uint64_t pack;
    coding::read_raw(file_, pack);
    record.type = static_cast<OperationType>(pack & 0xFF);
    record.seq = pack >> 8;

    uint32_t value_size;
    coding::read_raw(file_, value_size);
    record.value = coding::read_slice(file_, value_size);

    return record;
}


std::string mini_kv_store::generate_sst_filename(uint64_t id) const
{
    std::ostringstream oss;
    // Force 6-digit zero-padded alignment, ensuring OS-level lexicographical order is physical time order
    oss << std::setfill('0') <<std::setw(6) << id << ".sst";
    return oss.str();
}

void mini_kv_store::save_manifest() const
{
    const std::string tmp_manifest = "MANIFEST.tmp";
    const std::string real_manifest = "MANIFEST";

    std::ofstream out(tmp_manifest, std::ios::binary | std::ios::trunc);
    if (!out.is_open()) throw std::runtime_error("Cannot open MANIFEST.tmp");

    // 1. Persist global file sequence number
    const uint64_t current_id = next_sst_id_.load(std::memory_order_relaxed);
    coding::write_raw(out, current_id);

    // 2. Persist multi-dimensional file matrix
    const uint32_t num_levels = levels_.size();
    coding::write_raw(out, num_levels);

    for (const auto& level: levels_)
    {
        uint32_t num_files = level.size();
        coding::write_raw(out, num_files);
        for (uint64_t sst_id : level)
        {
            coding::write_raw(out, sst_id);
        }
    }

    out.flush();
    if (!out) throw std::runtime_error("Cannot write MANIFEST");
    out.close();
    if (!out) throw std::runtime_error("Cannot close MANIFEST");

    // 3. OS-level atomic replacement: force absolute consistency of the filesystem
#ifdef _WIN32
    const auto source = std::filesystem::path(tmp_manifest).wstring();
    const auto target = std::filesystem::path(real_manifest).wstring();
    if (!MoveFileExW(source.c_str(), target.c_str(),
                     MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH))
        throw std::runtime_error("Cannot replace MANIFEST");
#else
    std::filesystem::rename(tmp_manifest, real_manifest);
#endif
}

void mini_kv_store::load_manifest_and_rebuild_cache()
{
    const std::string manifest_path = "MANIFEST";
    if (!std::filesystem::exists(manifest_path)) return;

    std::ifstream in(manifest_path, std::ios::binary);
    if (!in.is_open()) return;

    // 1. Rebuild global sequence number
    uint64_t loaded_id {1};
    coding::read_raw(in, loaded_id);
    next_sst_id_.store(loaded_id, std::memory_order_relaxed);

    // 2. Rebuild multi-dimensional file matrix
    uint32_t num_levels = 0;
    coding::read_raw(in, num_levels);

    levels_.resize(num_levels);
    for (uint32_t i{0}; i < num_levels; ++i)
    {
        uint32_t num_files = 0;
        coding::read_raw(in, num_files);
        levels_[i].resize(num_files);
        for (uint32_t j{0}; j < num_files; ++j)
        {
            coding::read_raw(in, levels_[i][j]);
        }
    }
    in.close();

    // Brute-force rebuild bloom filter cache and index cache
    for (const auto& level : levels_)
    {
        for (uint64_t sst_id : level)
        {
            std::string sst_path = generate_sst_filename(sst_id);
            std::ifstream sst_file(sst_path, std::ios::binary | std::ios::ate);
            if (!sst_file.is_open()) throw std::runtime_error("MANIFEST references missing SST");

            uint64_t file_size = sst_file.tellg();
            if (file_size < 16) throw std::runtime_error("MANIFEST references truncated SST");

            // Read 16-byte Footer
            sst_file.seekg(-16, std::ios::end);
            uint64_t index_offset, bloom_offset;
            coding::read_raw(sst_file, index_offset);
            coding::read_raw(sst_file, bloom_offset);
            if (!sst_file || index_offset > bloom_offset || bloom_offset > file_size - 16)
                throw std::runtime_error("Invalid SST footer");

            // Calculate physical volume of bitmap and extract to memory
            uint64_t bloom_size = (file_size - 16) - bloom_offset;
            std::vector<uint8_t> bloom_data(bloom_size);

            sst_file.seekg(bloom_offset, std::ios::beg);
            coding::read_buffer(sst_file, std::span{bloom_data});

            bloom_cache_[sst_id] = std::move(bloom_data);

            // Physical extraction: deserialize disk sparse index into memory cache
            sst_file.seekg(index_offset, std::ios::beg);
            std::vector<index_record> sparse_index;
            // Absolute end of index block is start of bloom block (bloom_offset)
            while (true)
            {
                const auto position = sst_file.tellg();
                if (position == std::streampos(-1)) throw std::runtime_error("Invalid SST index");
                if (static_cast<uint64_t>(static_cast<std::streamoff>(position)) >= bloom_offset) break;
                uint32_t k_size;
                if (!coding::read_raw(sst_file, k_size)) throw std::runtime_error("Invalid SST index");
                std::string idx_key = coding::read_slice(sst_file, k_size);

                uint64_t block_offset;
                if (!sst_file || !coding::read_raw(sst_file, block_offset))
                    throw std::runtime_error("Invalid SST index");

                sparse_index.emplace_back(std::move(idx_key), block_offset);
            }
            index_cache_[sst_id] = std::move(sparse_index);
            sst_scanner scanner(sst_path);
            while (scanner.has_next())
            {
                const auto record = scanner.next();
                if (record.seq >= current_seq_.load(std::memory_order_relaxed))
                    current_seq_.store(record.seq + 1, std::memory_order_relaxed);
            }
        }
    }
}

uint32_t mini_kv_store::bloom_hash(std::string_view key, uint32_t seed)
{
    uint32_t h = seed;
    for (const char c : key)
    {
        h ^= static_cast<uint8_t>(c);
        h *= 0x01000193; // Physical constant: FNV prime
    }
    return h;
}

std::vector<uint8_t> mini_kv_store::generate_bloom_filter(const std::vector<std::string>& keys) const
{
    // Physical allocation: 10 bits per Key, round up to byte
    size_t num_bits = std::max(keys.size() * 10, static_cast<size_t>(64));
    size_t num_bytes = (num_bits + 7) / 8;
    std::vector<uint8_t> filter(num_bytes, 0);

    for (const auto& key: keys)
    {
        // Use 3 different hash seeds for physical burning
        for (const uint32_t seed: {0x12345678U, 0x87654321U, 0xABCDEF01U})
        {
            const uint32_t h = bloom_hash(key, seed) % (num_bytes * 8);
            filter[h / 8] |= (1 << (h % 8)); // Physical set
        }
    }
    return filter;
}

bool mini_kv_store::may_exist_in_sst(uint64_t sst_id, std::string_view key) const
{
    std::shared_lock lock(cache_mutex_);
    auto it = bloom_cache_.find(sst_id);
    if (it == bloom_cache_.end() || it->second.empty())
    {
        return true; // Cache miss, force pass to execute physical disk validation
    }

    const auto& filter = it -> second;
    size_t num_bits = filter.size() * 8;

    std::vector<uint32_t> seeds{0x12345678, 0x87654321, 0xABCDEF01};
    return std::ranges::none_of(seeds, [&](const uint32_t seed)
    {
        const uint32_t h = bloom_hash(key, seed) % num_bits;
        return (filter[h / 8] & (1 << (h % 8))) == 0; // Absolute physical interception: if any probe point is 0, data absolutely doesn't exist. Probabilistic pass: may exist, needs disk dive
    });
}

std::shared_ptr<const std::vector<std::byte>> mini_kv_store::fetch_block(const uint64_t sst_id, const uint64_t block_offset,
                                                                   const uint64_t next_offset) const
{
    const BlockPos pos{sst_id, block_offset};

    // --- Attempt cache interception ---
    {
        std::lock_guard lock(search_cache_.mtx_);
        if (const auto it = search_cache_.block_map_.find(pos); it != search_cache_.block_map_.end())
        {
            // Cache hit: execute physical timeline reconstruction
            search_cache_.lru_queue_.splice(search_cache_.lru_queue_.begin(), search_cache_.lru_queue_, it->second);             // std::list::splice can instantly sever and splice node to head without triggering any memory copy
            // 1. Extract shared_ptr<block_elem> of current node
            std::shared_ptr<lru_cache::block_elem> elem_ptr = *(it->second);
            // 2. Physical-level redirection
            return {elem_ptr, &(elem_ptr->data)};
        }
    }

    // --- Cache miss, fetch from disk ---
    const auto sst_path = generate_sst_filename(sst_id);
    std::ifstream sst_file(sst_path, std::ios::binary);
    if (!sst_file.is_open()) return nullptr;
    const uint64_t block_size = next_offset - block_offset;
    std::vector<std::byte> block_data(block_size);
    sst_file.seekg(block_offset, std::ios::beg);
    if (!coding::read_buffer(sst_file, std::span{block_data}))
        throw std::runtime_error("Cannot read SST block");
    sst_file.close();
    // Anchor new data block to LRU queue and map
    std::shared_ptr<lru_cache::block_elem> new_elem;
    {
        std::lock_guard lock(search_cache_.mtx_);

        // If capacity is exceeded, erase the tail element
        if (search_cache_.block_map_.size() >= search_cache_.capacity_)
        {
            auto last = search_cache_.lru_queue_.end(); --last;
            search_cache_.block_map_.erase(last->get()->pos);
            search_cache_.lru_queue_.pop_back();
        }

        // Enter block cache

        new_elem = std::make_shared<lru_cache::block_elem>(lru_cache::block_elem{pos, std::move(block_data)});
        search_cache_.lru_queue_.push_front(new_elem);
        search_cache_.block_map_[pos] = search_cache_.lru_queue_.begin();
    }
    return {new_elem, &(new_elem->data)};
}

SearchResult mini_kv_store::search_in_sstable(const std::string_view key, const uint64_t sst_id) const
{
    // --- Open .sst file ---
    const auto sst_path = generate_sst_filename(sst_id);
    std::ifstream sst_file(sst_path, std::ios::binary);
    if (!sst_file.is_open()) return {SearchStatus::FNotOpen, ""};

    // --- Use sparse index cache to find key block ---
    std::array<std::pair<uint64_t, uint64_t>, 2> candidate_blocks{};
    size_t candidate_count = 0;
    {
        std::shared_lock lock(cache_mutex_);
        const auto& sparse_index = index_cache_.at(sst_id);
        if (sparse_index.empty()) return {SearchStatus::kNotFound, ""};
        auto first_ge = std::lower_bound(sparse_index.begin(), sparse_index.end(), key,
            [](const index_record& elem, const std::string_view k)
            {
                return elem.key < k;
            });
        if (first_ge == sparse_index.begin() && first_ge->key > key)
            return {SearchStatus::kNotFound, ""};
        size_t first_index = first_ge == sparse_index.begin() ? 0 :
                             static_cast<size_t>(first_ge - sparse_index.begin() - 1);
        if (first_ge == sparse_index.end()) first_index = sparse_index.size() - 1;
        const bool check_equal_block = first_ge != sparse_index.end() &&
                                       first_ge != sparse_index.begin() && first_ge->key == key;
        sst_file.seekg(-static_cast<std::streamoff>(sizeof(uint64_t) * 2), std::ios::end);
        uint64_t data_end = 0;
        if (!coding::read_raw(sst_file, data_end)) throw std::runtime_error("Invalid SST footer");
        auto add_block = [&](size_t index)
        {
            const uint64_t end = index + 1 < sparse_index.size() ? sparse_index[index + 1].offset : data_end;
            candidate_blocks[candidate_count++] = {sparse_index[index].offset, end};
        };
        add_block(first_index);
        if (check_equal_block) add_block(first_index + 1);
    }

    for (size_t candidate = 0; candidate < candidate_count; ++candidate)
    {
        const auto [target_block_offset, next_block_offset] = candidate_blocks[candidate];
        auto block_data_ptr = fetch_block(sst_id, target_block_offset, next_block_offset);
        if (!block_data_ptr) return {SearchStatus::FNotOpen, ""};

        size_t offset = 0;
        std::span buffer{*block_data_ptr};
        while (offset < buffer.size())
        {
        const auto internal_key_size = coding::decode_fixed<uint32_t>(buffer.subspan(offset).first<sizeof(uint32_t)>());
        offset += sizeof(internal_key_size);

        const auto real_key_size = internal_key_size - sizeof(uint64_t);
        // no offset!

        const auto key_span = buffer.subspan(offset, real_key_size);
        const std::string_view current_key(reinterpret_cast<const char*>(key_span.data()), real_key_size);
        offset += real_key_size;

        const auto pack = coding::decode_fixed<uint64_t>(buffer.subspan(offset).first<sizeof(uint64_t)>());
        const auto type = static_cast<OperationType>(pack & 0xFF);
        offset += sizeof(pack);

        const auto value_size = coding::decode_fixed<uint32_t>(buffer.subspan(offset).first<sizeof(uint32_t)>());
        offset += sizeof(value_size);

        const auto value_span = buffer.subspan(offset, value_size);
        const std::string_view value(reinterpret_cast<const char*>(value_span.data()), value_size);
        offset += value_size;

        // Execute final spatio-temporal partial order comparison
        if (current_key == key)
        {
            if (type == OperationType::kDeletion)
            {
                return {SearchStatus::kFoundDeletion, ""};
            }
            return {SearchStatus::kFoundValue, std::string(value)}; // Cache decapsulation successful
        }

        // Data block itself is ordered, once crossing the target range it can be stopped early
            if (current_key.compare(key) > 0) break;
        }
    }

    return {SearchStatus::kNotFound, ""};
}
