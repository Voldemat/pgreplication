#include "./twophase.hpp"

#include <cstdint>
#include <expected>
#include <optional>
#include <span>
#include <string>
#include <variant>

#include "./utils.hpp"
#include "pgreplication/utils.hpp"

using namespace PGREPLICATION_NAMESPACE::utils;
namespace PGREPLICATION_NAMESPACE::pgoutput::events {
std::optional<TwoPhaseCommitEventType> parseTwoPhaseCommitEventType(
    const char &c) {
    const auto &casted = static_cast<TwoPhaseCommitEventType>(c);
    switch (casted) {
        case TwoPhaseCommitEventType::BEGIN_PREPARE:
        case TwoPhaseCommitEventType::PREPARE:
        case TwoPhaseCommitEventType::COMMIT_PREPARED:
        case TwoPhaseCommitEventType::ROLLBACK_PREPARED:
            return casted;
        default:
            return std::nullopt;
    };
};

BeginPrepare BeginPrepare::fromBuffer(const input_buffer &buffer) {
    return {
        .lsn = int64FromNetwork(buffer.subspan<0, 8>()),
        .endLsn = int64FromNetwork(buffer.subspan<8, 8>()),
        .timestamp = int64FromNetwork(buffer.subspan<16, 8>()),
        .transactionId = int32FromNetwork(buffer.subspan<24, 4>()),
        .gid = std::string(buffer.subspan<28>().data()),
    };
};

Prepare Prepare::fromBuffer(const input_buffer &buffer) {
    return { .flags = static_cast<std::int8_t>(buffer.subspan<0, 1>().front()),
             .lsn = int64FromNetwork(buffer.subspan<1, 8>()),
             .endLsn = int64FromNetwork(buffer.subspan<9, 8>()),
             .timestamp = int64FromNetwork(buffer.subspan<17, 8>()),
             .transactionId = int32FromNetwork(buffer.subspan<25, 4>()),
             .gid = std::string(buffer.subspan<29>().data()) };
};

CommitPrepared CommitPrepared::fromBuffer(const input_buffer &buffer) {
    return { .flags = static_cast<std::int8_t>(buffer.subspan<0, 1>().front()),
             .lsn = int64FromNetwork(buffer.subspan<1, 8>()),
             .endLsn = int64FromNetwork(buffer.subspan<9, 8>()),
             .timestamp = int64FromNetwork(buffer.subspan<17, 8>()),
             .transactionId = int32FromNetwork(buffer.subspan<25, 4>()),
             .gid = std::string(buffer.subspan<29>().data()) };
};

RollbackPrepared RollbackPrepared::fromBuffer(const input_buffer &buffer) {
    return { .flags = static_cast<std::int8_t>(buffer.subspan<0, 1>().front()),
             .lsn = int64FromNetwork(buffer.subspan<1, 8>()),
             .endLsn = int64FromNetwork(buffer.subspan<9, 8>()),
             .prepareTimestamp = int64FromNetwork(buffer.subspan<17, 8>()),
             .rollbackTimestamp = int64FromNetwork(buffer.subspan<25, 8>()),
             .transactionId = int32FromNetwork(buffer.subspan<33, 4>()),
             .gid = std::string(buffer.subspan<37>().data()) };
};

std::expected<
    std::variant<BeginPrepare, Prepare, CommitPrepared, RollbackPrepared>,
    std::string>
parseTwoPhaseCommitEvent(const TwoPhaseCommitEventType &eventType,
                         const std::span<char> &buffer) {
    switch (eventType) {
        case TwoPhaseCommitEventType::BEGIN_PREPARE:
            return utils::parseDynamicSizeEvent<BeginPrepare>(buffer);
        case TwoPhaseCommitEventType::PREPARE:
            return utils::parseDynamicSizeEvent<Prepare>(buffer);
        case TwoPhaseCommitEventType::COMMIT_PREPARED:
            return utils::parseDynamicSizeEvent<CommitPrepared>(buffer);
        case TwoPhaseCommitEventType::ROLLBACK_PREPARED:
            return utils::parseDynamicSizeEvent<RollbackPrepared>(buffer);
    };
};

};  // namespace PGREPLICATION_NAMESPACE::pgoutput::events
