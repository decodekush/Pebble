#ifndef MINI_LEVELDB_BINARY_CODEC_H
#define MINI_LEVELDB_BINARY_CODEC_H

#include <bit>
#include <array>
#include <concepts>
#include <span>
#include <algorithm>
#include <istream>
#include <ostream>
#include <string>
#include <string_view>
#include <type_traits>
#include <cstddef>

namespace coding
{
    template<typename T>
    concept TrivialLayout = std::is_trivial_v<T> && std::is_standard_layout_v<T>; // Concept: Trivial layout

    template<typename T>
    concept StringLike = std::convertible_to<T, std::string_view>; // Concept: String-like

    template<typename T>
    concept ByteLike = std::is_trivial_v<std::remove_cv_t<T>> && sizeof(std::remove_cv_t<T>) == 1;

    // Memory mapping
    template<TrivialLayout T>
    constexpr std::array<std::byte, sizeof(T)> encode_fixed(T value) noexcept // Object serialization (T->array)
    {
        return std::bit_cast<std::array<std::byte, sizeof(T)>>(value);
    }
    template<TrivialLayout T>
    constexpr T decode_fixed(std::span<const std::byte, sizeof(T)> bytes) noexcept // Object deserialization (span->array->T)
    {
        std::array<std::byte, sizeof(T)> arr;
        std::ranges::copy(bytes, arr.begin());
        return std::bit_cast<T>(arr);
    }

    // Physical I/O
    template<TrivialLayout T>
    void write_raw(std::ostream& os, const T& value)
    {
        auto bytes = std::as_bytes(std::span{&value, 1});
        os.write(reinterpret_cast<const char*>(bytes.data()), bytes.size_bytes());
    }

    template<StringLike T>
    void write_slice(std::ostream& os, const T& value)
    {
        const std::string_view sv{value};
        os.write(sv.data(), static_cast<std::streamsize>(sv.size()));
    }

    template<ByteLike T>
    void write_buffer(std::ostream& os, std::span<const T> buffer)
    {
        if (buffer.empty()) return;
        auto bytes = std::as_bytes(buffer);
        os.write(reinterpret_cast<const char*>(bytes.data()), static_cast<std::streamsize>(bytes.size_bytes()));
    }

    template<TrivialLayout T>
    bool read_raw(std::istream& is, T& value_out)
    {
        auto bytes = std::as_writable_bytes(std::span{&value_out, 1});
        return static_cast<bool>(is.read(reinterpret_cast<char*>(bytes.data()), bytes.size_bytes()));
    }

    template<ByteLike T>
    bool read_buffer(std::istream& is, std::span<T> buffer)
    {
        if (buffer.empty()) return true;
        auto bytes = std::as_writable_bytes(buffer);
        return static_cast<bool>(is.read(reinterpret_cast<char*>(bytes.data()), static_cast<std::streamsize>(bytes.size_bytes())));
    }

    inline std::string read_slice(std::istream& is, size_t size)
    {
        std::string buffer(size, '\0');
        is.read(buffer.data(), static_cast<std::streamsize>(size));
        return buffer;
    }
}

#endif //MINI_LEVELDB_BINARY_CODEC_H