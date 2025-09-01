#include "./pgoutput.hpp"

#include <cstddef>
#include <cstdint>
#include <expected>
#include <optional>
#include <span>
#include <string>
#include <variant>
#include <vector>

#include "pgreplication/utils.hpp"

namespace PGREPLICATION_NAMESPACE::pgoutput {
std::optional<BaseEventType> parseBaseEvenType(const char &c) {
    const auto &casted = static_cast<BaseEventType>(c);
    switch (casted) {
        case BaseEventType::BEGIN:
        case BaseEventType::COMMIT:
        case BaseEventType::ORIGIN:
        case BaseEventType::RELATION:
        case BaseEventType::TYPE:
        case BaseEventType::INSERT:
        case BaseEventType::UPDATE:
        case BaseEventType::DELETE:
        case BaseEventType::TRUNCATE:
            return casted;
        default:
            return std::nullopt;
    };
};

std::optional<StreamingEventType> parseStreamingEventType(const char &c) {
    const auto &casted = static_cast<StreamingEventType>(c);
    switch (casted) {
        case StreamingEventType::STREAM_START:
        case StreamingEventType::STREAM_STOP:
        case StreamingEventType::STREAM_COMMIT:
        case StreamingEventType::STREAM_ABORT:
            return casted;
        default:
            return std::nullopt;
    };
};

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

Begin parseBeginEvent(const std::span<char, Begin::bufferSize> &buffer) {
    return {
        .finalTransactionLsn = utils::int64FromNetwork(buffer.subspan<0, 8>()),
        .commitTimestamp = utils::int64FromNetwork(buffer.subspan<8, 8>()),
        .transactionId = utils::int32FromNetwork(buffer.subspan<16, 4>()),
    };
};

Commit parseCommitEvent(const std::span<char, Commit::bufferSize> &buffer) {
    return {
        .flags = buffer.subspan<0, 1>().front(),
        .lsn = utils::int64FromNetwork(buffer.subspan<1, 8>()),
        .endLsn = utils::int64FromNetwork(buffer.subspan<9, 8>()),
        .timestamp = utils::int64FromNetwork(buffer.subspan<17, 8>()),
    };
};

Origin parseOriginEvent(const std::span<char> &buffer) {
    const auto &origin = buffer.subspan<8>();
    return { .commitLsn = utils::int64FromNetwork(buffer.subspan<0, 8>()),
             .origin = std::string(origin.data(), origin.size()) };
};

RelationColumn parseRelationColumn(const std::span<char> &buffer) {
    const auto &name = std::string(buffer.subspan(1).data());
    return {
        .flags = buffer.subspan<0, 1>().front(),
        .name = name,
        .oid = utils::int32FromNetwork(
            buffer.subspan(1 + 1 + name.size(), 4).subspan<0, 4>()),
        .typeModifier = utils::int32FromNetwork(
            buffer.subspan(1 + 1 + name.size() + 4, 4).subspan<0, 4>()),
    };
};

std::vector<RelationColumn> parseRelationColumns(
    const std::int16_t &columnCount, const std::span<char> &buffer) {
    std::vector<RelationColumn> columns;
    unsigned int bufferPosition = 0;
    for (std::int16_t index = 0; index < columnCount; index++) {
        const auto &column =
            parseRelationColumn(buffer.subspan(bufferPosition));
        columns.emplace_back(column);
        bufferPosition = RelationColumn::minBufferSize + column.name.size();
    };
    return columns;
};

template <>
Relation<true> parseRelationEvent(const std::span<char> &buffer) {
    const auto &transactionId = utils::int32FromNetwork(buffer.subspan<0, 4>());
    const auto &oid = utils::int32FromNetwork(buffer.subspan<4, 4>());
    const auto &relationNamespace = std::string(buffer.subspan(8).data());
    const auto &afterNamespaceIndex = 8 + 1 + relationNamespace.size();
    const auto &name = std::string(buffer.subspan(afterNamespaceIndex).data());
    const auto &afterNameIndex = afterNamespaceIndex + 1 + name.size();
    const auto &replicaIdentity = buffer.subspan(afterNameIndex, 1).front();
    const auto &columnCount = utils::int16FromNetwork(
        buffer.subspan(afterNameIndex + 1, 2).subspan<0, 2>());
    return { .transactionId = transactionId,
             .oid = oid,
             .relationNamespace = relationNamespace,
             .name = name,
             .replicaIdentity = replicaIdentity,
             .columns = parseRelationColumns(
                 columnCount, buffer.subspan(afterNameIndex + 2)) };
};

template <>
Relation<false> parseRelationEvent(const std::span<char> &buffer) {
    const auto &oid = utils::int32FromNetwork(buffer.subspan<0, 4>());
    const auto &relationNamespace = std::string(buffer.subspan(4).data());
    const auto &afterNamespaceIndex = 4 + 1 + relationNamespace.size();
    const auto &name = std::string(buffer.subspan(afterNamespaceIndex).data());
    const auto &afterNameIndex = afterNamespaceIndex + 1 + name.size();
    const auto &replicaIdentity = buffer.subspan(afterNameIndex, 1).front();
    const auto &columnCount = utils::int16FromNetwork(
        buffer.subspan(afterNameIndex + 1, 2).subspan<0, 2>());
    return { .oid = oid,
             .relationNamespace = relationNamespace,
             .name = name,
             .replicaIdentity = replicaIdentity,
             .columns = parseRelationColumns(
                 columnCount, buffer.subspan(afterNameIndex + 2)) };
};

template <>
Type<true> parseTypeEvent(const std::span<char> &buffer) {
    const auto &transactionId = utils::int32FromNetwork(buffer.subspan<0, 4>());
    const auto &oid = utils::int32FromNetwork(buffer.subspan<4, 4>());
    const auto &typeNamespace = std::string(buffer.subspan<8>().data());
    const auto &name =
        std::string(buffer.subspan(8 + 1 + typeNamespace.size()).data());
    return {
        .transactionId = transactionId,
        .oid = oid,
        .typeNamespace = typeNamespace,
        .name = name,
    };
};

template <>
Type<false> parseTypeEvent(const std::span<char> &buffer) {
    const auto &oid = utils::int32FromNetwork(buffer.subspan<0, 4>());
    const auto &typeNamespace = std::string(buffer.subspan<4>().data());
    const auto &name =
        std::string(buffer.subspan(4 + 1 + typeNamespace.size()).data());
    return {
        .oid = oid,
        .typeNamespace = typeNamespace,
        .name = name,
    };
};

template <>
Truncate<true> parseTruncateEvent(const std::span<char> &buffer) {
    const auto &transactionId = utils::int32FromNetwork(buffer.subspan<0, 4>());
    const auto &relationsCount =
        utils::int32FromNetwork(buffer.subspan<4, 4>());
    const auto &flags = buffer.subspan<8, 1>().front();
    const auto &oidsBuffer =
        buffer.subspan(9, relationsCount * sizeof(std::int32_t));
    std::vector<std::int32_t> oids(oidsBuffer.begin(), oidsBuffer.end());
    return { .transactionId = transactionId, .flags = flags, .oids = oids };
};

template <>
Truncate<false> parseTruncateEvent(const std::span<char> &buffer) {
    const auto &relationsCount =
        utils::int32FromNetwork(buffer.subspan<0, 4>());
    const auto &flags = buffer.subspan<4, 1>().front();
    const auto &oidsBuffer =
        buffer.subspan(5, relationsCount * sizeof(std::int32_t));
    std::vector<std::int32_t> oids(oidsBuffer.begin(), oidsBuffer.end());
    return { .flags = flags, .oids = oids };
};

template <>
Message<true> parseMessageEvent(const std::span<char> &buffer) {
    const auto &transactionId = utils::int32FromNetwork(buffer.subspan<0, 4>());
    const auto &flags = buffer.subspan<4, 1>().front();
    const auto &lsn = utils::int64FromNetwork(buffer.subspan<5, 8>());
    const auto &prefix = std::string(buffer.subspan<13>().data());
    const auto &contentLength = utils::int32FromNetwork(
        buffer.subspan(13 + 1 + prefix.size(), 4).subspan<0, 4>());
    const auto &contentBuffer =
        buffer.subspan(13 + 1 + prefix.size() + 4, contentLength);
    std::vector<std::byte> content(
        reinterpret_cast<std::byte *>(contentBuffer.begin().base()),
        reinterpret_cast<std::byte *>(contentBuffer.end().base()));
    return {
        .transactionId = transactionId,
        .flags = flags,
        .lsn = lsn,
        .prefix = prefix,
        .content = content,
    };
};

template <>
Message<false> parseMessageEvent(const std::span<char> &buffer) {
    const auto &flags = buffer.subspan<0, 1>().front();
    const auto &lsn = utils::int64FromNetwork(buffer.subspan<1, 8>());
    const auto &prefix = std::string(buffer.subspan<9>().data());
    const auto &contentLength = utils::int32FromNetwork(
        buffer.subspan(9 + 1 + prefix.size(), 4).subspan<0, 4>());
    const auto &contentBuffer =
        buffer.subspan(9 + 1 + prefix.size() + 4, contentLength);
    std::vector<std::byte> content(
        reinterpret_cast<std::byte *>(contentBuffer.begin().base()),
        reinterpret_cast<std::byte *>(contentBuffer.end().base()));
    return {
        .flags = flags,
        .lsn = lsn,
        .prefix = prefix,
        .content = content,
    };
};

StreamStart parseStreamStart(
    const std::span<char, StreamStart::bufferSize> &buffer) {
    return {
        .transactionId = utils::int32FromNetwork(buffer.subspan<0, 4>()),
        .flags = buffer.subspan<4, 1>().front(),
    };
};

StreamCommit parseStreamCommit(
    const std::span<char, StreamCommit::bufferSize> &buffer) {
    return { .transactionId = utils::int32FromNetwork(buffer.subspan<0, 4>()),
             .flags = buffer.subspan<4, 1>().front(),
             .lsn = utils::int64FromNetwork(buffer.subspan<5, 8>()),
             .endLsn = utils::int64FromNetwork(buffer.subspan<13, 8>()),
             .timestamp = utils::int64FromNetwork(buffer.subspan<21, 8>()) };
};

template <>
StreamAbort<true> parseStreamAbort(
    const std::span<char, StreamAbort<true>::bufferSize> &buffer) {
    return { .transactionId = utils::int32FromNetwork(buffer.subspan<0, 4>()),
             .subTransactionId =
                 utils::int32FromNetwork(buffer.subspan<4, 4>()),
             .lsn = utils::int32FromNetwork(buffer.subspan<8, 4>()),
             .timestamp = utils::int64FromNetwork(buffer.subspan<12, 8>()) };
};

template <>
StreamAbort<false> parseStreamAbort(
    const std::span<char, StreamAbort<false>::bufferSize> &buffer) {
    return { .transactionId = utils::int32FromNetwork(buffer.subspan<0, 4>()),
             .subTransactionId =
                 utils::int32FromNetwork(buffer.subspan<4, 4>()) };
};

BeginPrepare parseBeginPrepare(const std::span<char> &buffer) {
    return {
        .lsn = utils::int64FromNetwork(buffer.subspan<0, 8>()),
        .endLsn = utils::int64FromNetwork(buffer.subspan<8, 8>()),
        .timestamp = utils::int64FromNetwork(buffer.subspan<16, 8>()),
        .transactionId = utils::int32FromNetwork(buffer.subspan<24, 4>()),
        .gid = std::string(buffer.subspan<28>().data()),
    };
};

Prepare parsePrepare(const std::span<char> &buffer) {
    return { .flags = buffer.subspan<0, 1>().front(),
             .lsn = utils::int64FromNetwork(buffer.subspan<1, 8>()),
             .endLsn = utils::int64FromNetwork(buffer.subspan<9, 8>()),
             .timestamp = utils::int64FromNetwork(buffer.subspan<17, 8>()),
             .transactionId = utils::int32FromNetwork(buffer.subspan<25, 4>()),
             .gid = std::string(buffer.subspan<29>().data()) };
};

CommitPrepared parseCommitPrepared(const std::span<char> &buffer) {
    return { .flags = buffer.subspan<0, 1>().front(),
             .lsn = utils::int64FromNetwork(buffer.subspan<1, 8>()),
             .endLsn = utils::int64FromNetwork(buffer.subspan<9, 8>()),
             .timestamp = utils::int64FromNetwork(buffer.subspan<17, 8>()),
             .transactionId = utils::int32FromNetwork(buffer.subspan<25, 4>()),
             .gid = std::string(buffer.subspan<29>().data()) };
};

RollbackPrepared parseRollbackPrepared(const std::span<char> &buffer) {
    return { .flags = buffer.subspan<0, 1>().front(),
             .lsn = utils::int64FromNetwork(buffer.subspan<1, 8>()),
             .endLsn = utils::int64FromNetwork(buffer.subspan<9, 8>()),
             .prepareTimestamp = utils::int64FromNetwork(buffer.subspan<17, 8>()),
             .rollbackTimestamp =
                 utils::int64FromNetwork(buffer.subspan<25, 8>()),
             .transactionId = utils::int32FromNetwork(buffer.subspan<33, 4>()),
             .gid = std::string(buffer.subspan<37>().data()) };
};

std::expected<
    std::variant<BeginPrepare, Prepare, CommitPrepared, RollbackPrepared>,
    std::string>
parseTwoPhaseCommitEvent(const TwoPhaseCommitEventType &eventType,
                         const std::span<char> &buffer) {
    switch (eventType) {
        case TwoPhaseCommitEventType::BEGIN_PREPARE:
            return parseDynamicSizeEvent<BeginPrepare>(buffer,
                                                       parseBeginPrepare);
        case TwoPhaseCommitEventType::PREPARE:
            return parseDynamicSizeEvent<Prepare>(buffer, parsePrepare);
        case TwoPhaseCommitEventType::COMMIT_PREPARED:
            return parseDynamicSizeEvent<CommitPrepared>(buffer,
                                                         parseCommitPrepared);
        case TwoPhaseCommitEventType::ROLLBACK_PREPARED:
            return parseDynamicSizeEvent<RollbackPrepared>(
                buffer, parseRollbackPrepared);
    };
};

StreamPrepare parseStreamPrepare(const std::span<char> &buffer) {
    return { .flags = buffer.subspan<0, 1>().front(),
             .lsn = utils::int64FromNetwork(buffer.subspan<1, 8>()),
             .endLsn = utils::int64FromNetwork(buffer.subspan<9, 8>()),
             .timestamp = utils::int64FromNetwork(buffer.subspan<17, 8>()),
             .transactionId = utils::int32FromNetwork(buffer.subspan<25, 4>()),
             .gid = std::string(buffer.subspan<29>().data()) };
};

};  // namespace PGREPLICATION_NAMESPACE::pgoutput
