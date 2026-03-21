#include "mini_leveldb/compaction.h"
#include "mini_leveldb/mini_kv_store.h"
#include "mini_leveldb/coding.h"
#include <queue>
#include <stdexcept>
#include <iostream>

// Helper to perform K-way merge of a set of files into a single new file
// Returns the ID of the new file, or 0 if no records were written (e.g. all tombstones deleted).
uint64_t CompactionStrategy::execute_compaction(mini_kv_store* store, const std::vector<uint64_t>& files_to_compact, bool drop_tombstones) {
        if (files_to_compact.empty()) return 0;
        
        uint64_t new_sst_id = store->next_sst_id_.fetch_add(1, std::memory_order_relaxed);
        std::string new_sst_path = store->generate_sst_filename(new_sst_id);
        std::ofstream new_sst_file(new_sst_path, std::ios::binary | std::ios::trunc);
        if (!new_sst_file) throw std::runtime_error("Cannot open compaction SST");
        
        std::vector<mini_kv_store::index_record> sparse_index;
        uint64_t current_offset = 0;
        std::vector<std::string> all_keys_for_bloom;
        
        struct HeapNode {
            ParsedRecord record;
            size_t scanner_idx;
            bool operator>(const HeapNode& other) const {
                const int cmp = record.key.compare(other.record.key);
                if (cmp == 0) return record.seq < other.record.seq;
                return cmp > 0;
            }
        };

        std::vector<mini_kv_store::sst_scanner> scanners;
        std::priority_queue<HeapNode, std::vector<HeapNode>, std::greater<>> min_heap;

        for (size_t i = 0; i < files_to_compact.size(); ++i) {
            scanners.emplace_back(store->generate_sst_filename(files_to_compact[i]));
            if (scanners.back().has_next()) {
                min_heap.push({scanners.back().next(), i});
            }
        }

        std::string last_key;
        bool have_last_key = false;
        uint64_t last_block_offset = 0;
        
        while (!min_heap.empty()) {
            HeapNode top = min_heap.top();
            min_heap.pop();
            
            if (!have_last_key || top.record.key != last_key) {
                last_key = top.record.key;
                have_last_key = true;

                if (!drop_tombstones || top.record.type != OperationType::kDeletion) {
                    if (constexpr uint64_t PAGE_SIZE = 4096; current_offset - last_block_offset >= PAGE_SIZE || current_offset == 0) {
                        sparse_index.emplace_back(mini_kv_store::index_record{top.record.key, current_offset});
                        last_block_offset = current_offset;
                    }

                    all_keys_for_bloom.push_back(top.record.key);

                    uint32_t internal_key_size = top.record.key.size() + sizeof(uint64_t);
                    coding::write_raw(new_sst_file, internal_key_size);
                    coding::write_slice(new_sst_file, top.record.key);
                    uint64_t pack = (top.record.seq << 8) | static_cast<uint8_t>(top.record.type);
                    coding::write_raw(new_sst_file, pack);
                    uint32_t value_size = top.record.value.size();
                    coding::write_raw(new_sst_file, value_size);
                    coding::write_slice(new_sst_file, top.record.value);

                    current_offset = new_sst_file.tellp();
                }
            }
            
            if (scanners[top.scanner_idx].has_next()) {
                min_heap.push({scanners[top.scanner_idx].next(), top.scanner_idx});
            }
        }

        uint64_t index_block_offset = current_offset;
        for (const auto& idx : sparse_index) {
            uint32_t k_size = idx.key.size();
            coding::write_raw(new_sst_file, k_size);
            coding::write_slice(new_sst_file, idx.key);
            coding::write_raw(new_sst_file, idx.offset);
        }

        uint64_t bloom_block_offset = new_sst_file.tellp();
        std::vector<uint8_t> bloom_data = store->generate_bloom_filter(all_keys_for_bloom);
        coding::write_buffer(new_sst_file, std::span<const uint8_t>{bloom_data});

        coding::write_raw(new_sst_file, index_block_offset);
        coding::write_raw(new_sst_file, bloom_block_offset);
        new_sst_file.flush();
        new_sst_file.close();
        
        bool has_compacted_records = !sparse_index.empty();
        
        if (has_compacted_records) {
            std::unique_lock cache_lock(store->cache_mutex_);
            store->bloom_cache_[new_sst_id] = std::move(bloom_data);
            store->index_cache_[new_sst_id] = std::move(sparse_index);
            return new_sst_id;
        } else {
            std::remove(new_sst_path.c_str());
            return 0;
    }
}

void LeveledCompaction::compact(mini_kv_store* store)
{
    std::vector<uint64_t> target_l0, target_l1;
    {
        std::unique_lock lock(store->manifest_mutex_);
        if (store->levels_.empty() || store->levels_[0].size() < 4) return;
        target_l0 = store->levels_[0];
        if (store->levels_.size() > 1) {
            target_l1 = store->levels_[1];
        }
    }

    std::vector<uint64_t> all_files = target_l0;
    all_files.insert(all_files.end(), target_l1.begin(), target_l1.end());
    
    uint64_t new_sst_id = execute_compaction(store, all_files, true);

    {
        std::unique_lock manifest_lock{store->manifest_mutex_};
        store->levels_[0].clear();
        
        if (store->levels_.size() < 2) store->levels_.resize(2);
        store->levels_[1].clear();
        if (new_sst_id != 0) {
            store->levels_[1].push_back(new_sst_id);
        }
        
        try { store->save_manifest(); }
        catch (...) {
            store->levels_[0] = target_l0;
            store->levels_[1] = target_l1;
            throw;
        }
        
        for (uint64_t old_id : all_files) {
            std::remove(store->generate_sst_filename(old_id).c_str());
        }
    }
}

void SizeTieredCompaction::compact(mini_kv_store* store)
{
    // Size-tiered compaction merges N files in a level into 1 file in the next level
    // We trigger compaction recursively downwards.
    while (true) {
        int compact_level = -1;
        std::vector<uint64_t> files_to_compact;
        
        {
            std::unique_lock lock(store->manifest_mutex_);
            for (size_t i = 0; i < store->levels_.size(); ++i) {
                if (store->levels_[i].size() >= 4) { // Threshold for merging
                    compact_level = i;
                    files_to_compact = store->levels_[i];
                    break;
                }
            }
        }
        
        if (compact_level == -1) break;
        
        size_t next_level = compact_level + 1;
        bool is_last_level = true;
        {
            std::unique_lock lock(store->manifest_mutex_);
            for (size_t i = next_level; i < store->levels_.size(); ++i) {
                if (!store->levels_[i].empty()) {
                    is_last_level = false;
                    break;
                }
            }
        }
        
        uint64_t new_sst_id = execute_compaction(store, files_to_compact, is_last_level);
        
        {
            std::unique_lock manifest_lock{store->manifest_mutex_};
            store->levels_[compact_level].clear();
            
            if (store->levels_.size() <= next_level) {
                store->levels_.resize(next_level + 1);
            }
            if (new_sst_id != 0) {
                store->levels_[next_level].push_back(new_sst_id);
            }
            
            try { store->save_manifest(); }
            catch (...) {
                store->levels_[compact_level] = files_to_compact;
                if (new_sst_id != 0) store->levels_[next_level].pop_back();
                throw;
            }
            
            for (uint64_t old_id : files_to_compact) {
                std::remove(store->generate_sst_filename(old_id).c_str());
            }
        }
    }
}

