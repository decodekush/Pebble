#ifndef MINI_LEVELDB_COMPACTION_H
#define MINI_LEVELDB_COMPACTION_H

#include <vector>
#include <cstdint>

class mini_kv_store;

class CompactionStrategy
{
public:
    virtual ~CompactionStrategy() = default;
    
    // Executes the compaction strategy on the store.
    virtual void compact(mini_kv_store* store) = 0;
protected:
    uint64_t execute_compaction(mini_kv_store* store, const std::vector<uint64_t>& files_to_compact, bool drop_tombstones = false);
};

class LeveledCompaction : public CompactionStrategy
{
public:
    void compact(mini_kv_store* store) override;
};

class SizeTieredCompaction : public CompactionStrategy
{
public:
    void compact(mini_kv_store* store) override;
};

#endif // MINI_LEVELDB_COMPACTION_H
