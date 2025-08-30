#pragma once

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <span>
#include <string>

namespace PGREPLICATION_NAMESPACE {

enum class EventType {
    XLogData = 'w',
    PrimaryKeepaliveMessage = 'k',
    StandbyStatusUpdate = 'r',
    HostStandbyFeedbackMessage = 'h',
};

struct XLogData {
    std::int64_t messageWalStart;
    std::int64_t serverWalEnd;
    std::int64_t sentAtUnixTimestamp;
    std::span<char> walData;

    constexpr static const std::size_t minSize = sizeof(std::int64_t) * 3;

    static std::expected<XLogData, std::string> fromNetworkBuffer(
        const std::span<char> &buffer);
};

struct PrimaryKeepaliveMessage {
    std::int64_t serverWalEnd;
    std::int64_t sentAtUnixTimestamp;
    bool replyRequested;

    constexpr static const std::size_t size =
        sizeof(std::int64_t) * 2 + sizeof(bool);

    static PrimaryKeepaliveMessage fromNetworkBuffer(
        const std::span<char, size> &buffer);
};

struct StandbyStatusUpdate {
    std::int64_t writtenWalPosition;
    std::int64_t flushedWalPosition;
    std::int64_t appliedWalPosition;
    std::int64_t sentAtUnixTimestamp;
    bool replyRequested;

    constexpr static const std::size_t size =
        sizeof(std::int64_t) * 4 + sizeof(bool);
    using network_buffer = std::span<char, size>;

    void toNetworkBuffer(const network_buffer &buffer);

    static StandbyStatusUpdate fromNetworkBuffer(const network_buffer &buffer);
};

struct HotStandbyFeedbackMessage {
    std::int64_t sentAtUnixTimestamp;
    std::int32_t xmin;
    std::int32_t xminEpoch;
    std::int32_t lowestReplicationSlotCatalogXmin;
    std::int32_t catalogXminEpoch;

    constexpr static const std::size_t size =
        sizeof(std::int64_t) + sizeof(std::int32_t) * 4;

    static HotStandbyFeedbackMessage fromNetworkBuffer(
        const std::span<char, size> &buffer);
};

};  // namespace PGREPLICATION_NAMESPACE
