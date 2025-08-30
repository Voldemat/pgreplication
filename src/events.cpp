#include "./events.hpp"

#include <arpa/inet.h>

#include <cassert>
#include <cstddef>
#include <cstring>
#include <expected>
#include <format>
#include <optional>
#include <span>
#include <string>
#include <variant>
#include <vector>

#include "utils.hpp"

namespace PGREPLICATION_NAMESPACE {
std::expected<XLogData, std::string> XLogData::fromNetworkBuffer(
    const std::span<char> &buffer) {
    if (buffer.size() <= minSize) {
        return std::unexpected(
            std::format("Size must be greater than {}, received: {}", minSize,
                        buffer.size()));
    };
    return (XLogData){
        .messageWalStart = utils::int64FromNetwork(buffer.subspan(0, 8)),
        .serverWalEnd = utils::int64FromNetwork(buffer.subspan(8, 8)),
        .sentAtUnixTimestamp = utils::int64FromNetwork(buffer.subspan(16, 8)),
        .walData = buffer.subspan(24),
    };
};

std::size_t XLogData::getNetworkBufferSize() const {
    return minSize + walData.size();
};

void XLogData::toNetworkBuffer(const network_buffer &buffer) const {
    assert(buffer.size() == getNetworkBufferSize());
    utils::int64ToNetwork(&buffer[0], messageWalStart);
    utils::int64ToNetwork(&buffer[8], serverWalEnd);
    utils::int64ToNetwork(&buffer[16], sentAtUnixTimestamp);
    std::memcpy(&buffer[24], walData.data(), walData.size());
};

PrimaryKeepaliveMessage PrimaryKeepaliveMessage::fromNetworkBuffer(
    const std::span<char, size> &buffer) {
    return (PrimaryKeepaliveMessage){
        .serverWalEnd = utils::int64FromNetwork(buffer.subspan(0, 8)),
        .sentAtUnixTimestamp = utils::int64FromNetwork(buffer.subspan(8, 8)),
        .replyRequested = utils::boolFromNetwork(buffer[16])
    };
};

void PrimaryKeepaliveMessage::toNetworkBuffer(
    const network_buffer &buffer) const {
    utils::int64ToNetwork(&buffer[0], serverWalEnd);
    utils::int64ToNetwork(&buffer[8], sentAtUnixTimestamp);
    utils::boolToNetwork(&buffer[16], replyRequested);
};

StandbyStatusUpdate StandbyStatusUpdate::fromNetworkBuffer(
    const std::span<char, size> &buffer) {
    return (StandbyStatusUpdate){
        .writtenWalPosition = utils::int64FromNetwork(buffer.subspan(0, 8)),
        .flushedWalPosition = utils::int64FromNetwork(buffer.subspan(8, 8)),
        .appliedWalPosition = utils::int64FromNetwork(buffer.subspan(16, 8)),
        .sentAtUnixTimestamp = utils::int64FromNetwork(buffer.subspan(24, 8)),
        .replyRequested = utils::boolFromNetwork(buffer[32])
    };
};

void StandbyStatusUpdate::toNetworkBuffer(const network_buffer &buffer) const {
    utils::int64ToNetwork(&buffer[0], writtenWalPosition);
    utils::int64ToNetwork(&buffer[8], flushedWalPosition);
    utils::int64ToNetwork(&buffer[16], appliedWalPosition);
    utils::int64ToNetwork(&buffer[24], sentAtUnixTimestamp);
    utils::boolToNetwork(&buffer[32], replyRequested);
};

HotStandbyFeedbackMessage HotStandbyFeedbackMessage::fromNetworkBuffer(
    const std::span<char, size> &buffer) {
    return (HotStandbyFeedbackMessage){
        .sentAtUnixTimestamp = utils::int64FromNetwork(buffer.subspan(0, 8)),
        .xmin = utils::int32FromNetwork(buffer.subspan(8, 4)),
        .xminEpoch = utils::int32FromNetwork(buffer.subspan(12, 4)),
        .lowestReplicationSlotCatalogXmin =
            utils::int32FromNetwork(buffer.subspan(16, 4)),
        .catalogXminEpoch = utils::int32FromNetwork(buffer.subspan(20, 4))
    };
};

void HotStandbyFeedbackMessage::toNetworkBuffer(
    const network_buffer &buffer) const {
    utils::int64ToNetwork(&buffer[0], sentAtUnixTimestamp);
    utils::int32ToNetwork(&buffer[8], xmin);
    utils::int32ToNetwork(&buffer[12], xminEpoch);
    utils::int32ToNetwork(&buffer[16], lowestReplicationSlotCatalogXmin);
    utils::int32ToNetwork(&buffer[20], catalogXminEpoch);
};

std::optional<PrimaryEventType> primaryEventTypeFromChar(const char &c) {
    const auto &casted = static_cast<PrimaryEventType>(c);
    switch (casted) {
        case PrimaryEventType::XLogData:
        case PrimaryEventType::PrimaryKeepaliveMessage:
            return casted;
        default:
            return std::nullopt;
    };
};

std::expected<PrimaryEvent, std::string> primaryEventFromNetworkBuffer(
    const std::span<char> &buffer) {
    assert(buffer.size() > 0);
    const auto &typeChar = buffer[0];
    const auto &typeResult = primaryEventTypeFromChar(typeChar);
    if (!typeResult.has_value()) {
        return std::unexpected(std::format("Unexpected type: {}", typeChar));
    };
    const auto &type = typeResult.value();
    const auto &eventBuffer = buffer.subspan(1);
    switch (type) {
        case PrimaryEventType::PrimaryKeepaliveMessage: {
            if (eventBuffer.size() != PrimaryKeepaliveMessage::size) {
                return std::unexpected(std::format(
                    "PrimaryKeepaliveMessage buffer size must be equal {}",
                    PrimaryKeepaliveMessage::size));
            };
            return PrimaryKeepaliveMessage::fromNetworkBuffer(
                eventBuffer.subspan<0, PrimaryKeepaliveMessage::size>());
        }
        case PrimaryEventType::XLogData: {
            if (eventBuffer.size() < XLogData::minSize) {
                return std::unexpected(std::format(
                    "XLogData buffer size must be gte {}", XLogData::minSize));
            };
            return XLogData::fromNetworkBuffer(eventBuffer);
        }
    };
};

std::vector<char> primaryKeepaliveMessageToNetworkBuffer(
    const PrimaryKeepaliveMessage &message) {
    std::vector<char> buffer(1 + message.size);
    buffer[0] = static_cast<char>(PrimaryEventType::PrimaryKeepaliveMessage);
    message.toNetworkBuffer(std::span<char, PrimaryKeepaliveMessage::size>(
        buffer.data() + 1, PrimaryKeepaliveMessage::size));
    return buffer;
};

std::vector<char> xLogDataToNetworkBuffer(const XLogData &data) {
    std::vector<char> buffer(1 + data.getNetworkBufferSize());
    buffer[0] = static_cast<char>(PrimaryEventType::XLogData);
    data.toNetworkBuffer(
        std::span(buffer.data() + 1, data.getNetworkBufferSize()));
    return buffer;
};

std::vector<char> primaryEventToNetworkBuffer(const PrimaryEvent &event) {
    return std::visit(
        utils::overloaded{
            [](const PrimaryKeepaliveMessage &message) -> std::vector<char> {
                return primaryKeepaliveMessageToNetworkBuffer(message);
            },
            [](const XLogData &data) -> std::vector<char> {
                return xLogDataToNetworkBuffer(data);
            } },
        event);
};

std::optional<StandbyEventType> standbyEventTypeFromChar(const char &c) {
    const auto &casted = static_cast<StandbyEventType>(c);
    switch (casted) {
        case StandbyEventType::StandbyStatusUpdate:
        case StandbyEventType::HotStandbyFeedbackMessage:
            return casted;
        default:
            return std::nullopt;
    };
};

std::expected<StandbyEvent, std::string> standbyEventFromNetworkBuffer(
    const std::span<char> &buffer) {
    assert(buffer.size() > 0);
    const auto &typeChar = buffer[0];
    const auto &typeResult = standbyEventTypeFromChar(typeChar);
    if (!typeResult.has_value()) {
        return std::unexpected(std::format("Unexpected type: {}", typeChar));
    };
    const auto &type = typeResult.value();
    const auto &eventBuffer = buffer.subspan(1);
    switch (type) {
        case StandbyEventType::StandbyStatusUpdate: {
            if (eventBuffer.size() != StandbyStatusUpdate::size) {
                return std::unexpected(std::format(
                    "StandbyStatusUpdate buffer size must be equal {}",
                    StandbyStatusUpdate::size));
            };
            return StandbyStatusUpdate::fromNetworkBuffer(
                eventBuffer.subspan<0, StandbyStatusUpdate::size>());
        }
        case StandbyEventType::HotStandbyFeedbackMessage: {
            if (eventBuffer.size() != HotStandbyFeedbackMessage::size) {
                return std::unexpected(std::format(
                    "HotStandbyFeedbackMessage buffer size must be equal {}",
                    HotStandbyFeedbackMessage::size));
            };
            return HotStandbyFeedbackMessage::fromNetworkBuffer(
                std::span<char, HotStandbyFeedbackMessage::size>(eventBuffer));
        }
    };
};

std::vector<char> standByStatusUpdateToNetworkBuffer(
    const StandbyStatusUpdate &message) {
    std::vector<char> buffer(1 + StandbyStatusUpdate::size);
    buffer[0] = static_cast<char>(StandbyEventType::StandbyStatusUpdate);
    message.toNetworkBuffer(std::span<char, StandbyStatusUpdate::size>(
        buffer.data() + 1, StandbyStatusUpdate::size));
    return buffer;
};

std::vector<char> hotStandbyFeedbackMessageToNetworkBuffer(
    const HotStandbyFeedbackMessage &message) {
    std::vector<char> buffer(1 + HotStandbyFeedbackMessage::size);
    buffer[0] = static_cast<char>(StandbyEventType::HotStandbyFeedbackMessage);
    message.toNetworkBuffer(std::span<char, HotStandbyFeedbackMessage::size>(
        buffer.data() + 1, HotStandbyFeedbackMessage::size));
    return buffer;
};

std::vector<char> standbyEventToNetworkBuffer(const StandbyEvent &event) {
    return std::visit(
        utils::overloaded{
            [](const StandbyStatusUpdate &message) -> std::vector<char> {
                return standByStatusUpdateToNetworkBuffer(message);
            },
            [](const HotStandbyFeedbackMessage &message) -> std::vector<char> {
                return hotStandbyFeedbackMessageToNetworkBuffer(message);
            } },
        event);
};

};  // namespace PGREPLICATION_NAMESPACE
