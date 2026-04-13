#ifndef MINI_LEVELDB_WIRE_PROTOCOL_H
#define MINI_LEVELDB_WIRE_PROTOCOL_H

#include <cstdint>
#include <string>
#include <vector>
#include <type_traits>

namespace net
{
    // Absolute physical constant: magic number interceptor ('K'), used to kill illegal port scans when parsing the first byte
    constexpr uint8_t PROTOCOL_MAGIC = 0x4B;

    // Opcode mapping
    enum class NetOpcode : uint8_t
    {
        kPut = 0x01,
        kGet = 0x02,
        kDelete = 0x03,
        kError = 0xFF
    };

    // Forcefully strip C++ struct memory alignment padding
#pragma pack(push, 1)
    struct MsgHeader
    {
        uint8_t magic; // 1B
        NetOpcode opcode; // 1B
        uint32_t key_len; // 4B
        uint32_t value_len; // 4B
    };
#pragma pack(pop)

// Compile-time physical validation: establish 10-byte absolute limit
static_assert(sizeof(MsgHeader) == 10, "FATAL: MsgHeader physical volume is abnormal, network deserialization will cause pointer dislocation");
static_assert(std::is_trivial_v<MsgHeader> && std::is_standard_layout_v<MsgHeader>, "FATAL: MsgHeader must be a trivial carrier to support underlying read_raw zero-copy");

    // Memory-state request unpacking carrier
    struct NetRequest
    {
        NetOpcode opcode;
        std::string key;
        std::string value;
    };

    // Memory-state receipt carrier
    struct NetResponse
    {
        uint8_t status; // 0x00: Success, 0x01: Failure/Not Found
        std::string value;
    };

    // TCP session lifecycle state machine
    enum class SessionState
    {
        READ_HEADER, // State 1: Extracting 10-byte fixed-length protocol header from Socket
        READ_BODY, // State 2: Extracting variable-length KV data stream based on header length
        PROCESS, // State 3: Packet physically closed, handed over to background Worker thread for compute
        WRITE_REPLY // State 4: Storage engine calculation completed, waiting for Epoll to wake up writable event and inject back to NIC
    };

} // namespace net

#endif //MINI_LEVELDB_WIRE_PROTOCOL_H