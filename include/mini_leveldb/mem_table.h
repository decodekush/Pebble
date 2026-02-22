#ifndef MINI_LEVELDB_ORDERED_MAP_H
#define MINI_LEVELDB_ORDERED_MAP_H

#include <vector>
#include <random>
#include <iterator>
#include <memory_resource>

#include <cstddef>
#include <span>
#include <algorithm>

#include "mini_leveldb/db_format.h"

// --- In-memory Skip List ---
class mem_table
{
    struct Node;
public:
    mem_table();
    ~mem_table();

    // --- Disable copy & move semantics ---
    mem_table(const mem_table&) = delete;
    mem_table& operator=(const mem_table&) = delete;
    mem_table(mem_table&&) = delete;
    mem_table& operator=(mem_table&&) = delete;

    // --- Iterators ---
    class skip_list_iterator
    {
        friend class mem_table;
        explicit skip_list_iterator (Node*);
    public:
        using iterator_category = std::forward_iterator_tag;
        using difference_type = std::ptrdiff_t;
        using value_type = ParsedRecordView;
        using reference = value_type;

        [[nodiscard]] reference operator*() const;
        [[nodiscard]] std::span<const std::byte> raw_payload() const;
        skip_list_iterator& operator++();
        skip_list_iterator operator++(int);
        bool operator==(const skip_list_iterator&) const;
    private:
        Node* current_;
    };
    using const_iterator = skip_list_iterator;
    [[nodiscard]] skip_list_iterator begin() const;
    [[nodiscard]] skip_list_iterator end() const;
    [[nodiscard]] const_iterator cbegin() const;

    // --- CURD ---
    [[nodiscard]] SearchResult get (std::string_view) const;
    void insert_record(std::string_view, uint64_t, std::string_view);
    void insert_tombstone(std::string_view, uint64_t);

    // --- Space consumption ---
    [[nodiscard]] size_t approximate_memory_usage() const;

private:
    // --- Node structure ---
    struct Node
    {
        Node(int, std::string_view, uint64_t, OperationType, std::string_view, std::pmr::polymorphic_allocator<>);
        std::pmr::vector<std::byte> payload_; // Heap byte payload
        std::pmr::vector<Node*> forward_; // Forward array
        [[nodiscard]] ParsedRecordView decode() const; // Return payload content
    };

    // --- Skip List structure ---
    Node* head_;
    int max_level_ = 15; // Max level limit (16 levels)
    float probability_ = 0.25; // Promotion probability
    int current_level_ = 0; // Current max level
    void internal_put(std::string_view, uint64_t, OperationType, std::string_view); // Insert node

    // --- Random number generation ---
    std::mt19937 eng_{std::random_device{}()}; // Independent random engine per memtable
    std::uniform_int_distribution<> dist_{0, 10};
    double get_random();

    // --- Memory management ---
    std::pmr::monotonic_buffer_resource arena_ {1024 * 1024}; // Memory pool
    std::pmr::polymorphic_allocator<> alloc_ {&arena_}; // Memory allocator
    size_t usage_ = 0; // Current memory usage
};

#endif //MINI_LEVELDB_ORDERED_MAP_H
