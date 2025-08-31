#pragma once

#include <array>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <optional>
#include <span>
#include <string>
#include <variant>
#include <vector>

namespace PGREPLICATION_NAMESPACE {

enum class PrimaryEventType { XLogData = 'w', PrimaryKeepaliveMessage = 'k' };

enum class StandbyEventType {
    StandbyStatusUpdate = 'r',
    HotStandbyFeedbackMessage = 'h',
};

struct XLogData {
    std::int64_t messageWalStart;
    std::int64_t serverWalEnd;
    std::int64_t sentAtUnixTimestamp;
    std::span<char> walData;

    constexpr static const std::size_t minSize = sizeof(std::int64_t) * 3;
    using network_buffer = std::span<char>;

    std::size_t getNetworkBufferSize() const;
    void toNetworkBuffer(const network_buffer &buffer) const;

    static std::expected<XLogData, std::string> fromNetworkBuffer(
        const network_buffer &buffer);
};

struct PrimaryKeepaliveMessage {
    std::int64_t serverWalEnd;
    std::int64_t sentAtUnixTimestamp;
    bool replyRequested;

    constexpr static const std::size_t size =
        sizeof(std::int64_t) * 2 + sizeof(bool);
    using network_buffer = std::span<char, size>;

    void toNetworkBuffer(const network_buffer &buffer) const;

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

    void toNetworkBuffer(const network_buffer &buffer) const;

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
    using network_buffer = std::span<char, size>;

    void toNetworkBuffer(const network_buffer &buffer) const;

    static HotStandbyFeedbackMessage fromNetworkBuffer(
        const network_buffer &buffer);
};

using PrimaryEvent = std::variant<XLogData, PrimaryKeepaliveMessage>;
using StandbyEvent =
    std::variant<StandbyStatusUpdate, HotStandbyFeedbackMessage>;

std::expected<PrimaryEvent, std::string> primaryEventFromNetworkBuffer(
    const std::span<char> &buffer);

std::optional<PrimaryEventType> primaryEventTypeFromChar(const char &c);

std::array<char, 1 + PrimaryKeepaliveMessage::size>
primaryKeepaliveMessageToNetworkBuffer(const PrimaryKeepaliveMessage &message);

std::vector<char> xLogDataToNetworkBuffer(const XLogData &data);

std::vector<char> primaryEventToNetworkBuffer(const PrimaryEvent &event);

std::optional<StandbyEventType> standbyEventTypeFromChar(const char &c);

std::expected<StandbyEvent, std::string> standbyEventFromNetworkBuffer(
    const std::span<char> &buffer);

std::array<char, 1 + StandbyStatusUpdate::size>
standByStatusUpdateToNetworkBuffer(const StandbyStatusUpdate &message);

std::array<char, 1 + HotStandbyFeedbackMessage::size>
hotStandbyFeedbackMessageToNetworkBuffer(
    const HotStandbyFeedbackMessage &message);

std::vector<char> standbyEventToNetworkBuffer(const StandbyEvent &event);
};  // namespace PGREPLICATION_NAMESPACE
