#include "./events.hpp"

#include <arpa/inet.h>

#include <cassert>
#include <expected>
#include <format>
#include <span>
#include <string>

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

PrimaryKeepaliveMessage PrimaryKeepaliveMessage::fromNetworkBuffer(
    const std::span<char, size> &buffer) {
    return (PrimaryKeepaliveMessage){
        .serverWalEnd = utils::int64FromNetwork(buffer.subspan(0, 8)),
        .sentAtUnixTimestamp = utils::int64FromNetwork(buffer.subspan(8, 8)),
        .replyRequested = utils::boolFromNetwork(buffer[16])
    };
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

void StandbyStatusUpdate::toNetworkBuffer(const network_buffer &buffer) {
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
};  // namespace PGREPLICATION_NAMESPACE
