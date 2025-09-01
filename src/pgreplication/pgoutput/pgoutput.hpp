#pragma once

#include <cassert>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <format>
#include <functional>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#include "pgreplication/utils.hpp"

namespace PGREPLICATION_NAMESPACE::pgoutput {

struct Begin {
    std::int64_t finalTransactionLsn;
    std::int64_t commitTimestamp;
    std::int32_t transactionId;

    constexpr static std::size_t bufferSize = sizeof(finalTransactionLsn) +
                                              sizeof(commitTimestamp) +
                                              sizeof(transactionId);
};

template <bool Streaming>
struct Message;

template <>
struct Message<true> {
    std::int32_t transactionId;
    std::int8_t flags;
    std::int64_t lsn;
    std::string prefix;
    std::vector<std::byte> content;
};

template <>
struct Message<false> {
    std::int8_t flags;
    std::int64_t lsn;
    std::string prefix;
    std::vector<std::byte> content;
};

struct Commit {
    std::int8_t flags;
    std::int64_t lsn;
    std::int64_t endLsn;
    std::int64_t timestamp;

    constexpr static std::size_t bufferSize =
        sizeof(flags) + sizeof(lsn) + sizeof(endLsn) + sizeof(timestamp);
};

struct Origin {
    std::int64_t commitLsn;
    std::string origin;

    constexpr static std::size_t minBufferSize = sizeof(commitLsn) + 1;
};

struct RelationColumn {
    std::int8_t flags;
    std::string name;
    std::int32_t oid;
    std::int32_t typeModifier;

    constexpr static std::size_t minBufferSize =
        sizeof(flags) + 1 + sizeof(oid) + sizeof(typeModifier);
};

template <bool Streaming>
struct Relation;

template <>
struct Relation<true> {
    std::int32_t transactionId;
    std::int32_t oid;
    std::string relationNamespace;
    std::string name;
    std::int8_t replicaIdentity;
    std::vector<RelationColumn> columns;

    constexpr static std::size_t minBufferSize =
        sizeof(transactionId) + sizeof(oid) + 1 + 1 + sizeof(replicaIdentity) +
        sizeof(std::int16_t);
};

template <>
struct Relation<false> {
    std::int32_t oid;
    std::string relationNamespace;
    std::string name;
    std::int8_t replicaIdentity;
    std::vector<RelationColumn> columns;
    constexpr static std::size_t minBufferSize =
        sizeof(oid) + 1 + 1 + sizeof(replicaIdentity) + sizeof(std::int16_t);
};

template <bool Streaming>
struct Type;

template <>
struct Type<true> {
    std::int32_t transactionId;
    std::int32_t oid;
    std::string typeNamespace;
    std::string name;

    constexpr static std::size_t minBufferSize =
        sizeof(transactionId) + sizeof(oid) + 1 + 1;
};

template <>
struct Type<false> {
    std::int32_t oid;
    std::string typeNamespace;
    std::string name;

    constexpr static std::size_t minBufferSize = sizeof(oid) + 1 + 1;
};

struct PGNull {};
struct PGUnchangedToastedValue {};

template <bool Binary>
using TupleDataColumn = std::variant<
    PGNull, PGUnchangedToastedValue,
    std::conditional_t<Binary, std::vector<std::byte>, std::string>>;

template <bool Binary>
using TupleData = std::vector<TupleDataColumn<Binary>>;

template <bool Binary, bool Streaming>
struct Insert;

template <bool Binary>
struct Insert<Binary, true> {
    std::int32_t transactionId;
    std::int32_t oid;
    TupleData<Binary> data;

    constexpr static std::size_t minBufferSize =
        sizeof(transactionId) + sizeof(oid) + sizeof(std::int16_t);
};

template <bool Binary>
struct Insert<Binary, false> {
    std::int32_t oid;
    TupleData<Binary> data;

    constexpr static std::size_t minBufferSize =
        sizeof(oid) + sizeof(std::int16_t);
};

template <bool Binary>
using OldTupleData = TupleData<Binary>;
template <bool Binary>
using PrimaryKeyTupleData = TupleData<Binary>;

template <bool Binary>
using OldDataOrPrimaryKeyTupleData =
    std::variant<OldTupleData<Binary>, PrimaryKeyTupleData<Binary>>;

template <bool Binary, bool Streaming>
struct Update;

template <bool Binary>
struct Update<Binary, true> {
    std::int32_t transactionId;
    std::int32_t oid;
    std::optional<OldDataOrPrimaryKeyTupleData<Binary>> oldDataOrPrimaryKey;
    TupleData<Binary> data;

    constexpr static std::size_t minBufferSize =
        sizeof(transactionId) + sizeof(oid) + sizeof(std::int16_t);
};

template <bool Binary>
struct Update<Binary, false> {
    std::int32_t oid;
    std::optional<OldDataOrPrimaryKeyTupleData<Binary>> oldDataOrPrimaryKey;
    TupleData<Binary> data;

    constexpr static std::size_t minBufferSize =
        sizeof(oid) + sizeof(std::int16_t);
};

template <bool Binary, bool Streaming>
struct Delete;

template <bool Binary>
struct Delete<Binary, true> {
    std::int32_t transactionId;
    std::int32_t oid;
    std::optional<OldDataOrPrimaryKeyTupleData<Binary>> oldDataOrPrimaryKey;
    constexpr static std::size_t minBufferSize =
        sizeof(transactionId) + sizeof(oid);
};

template <bool Binary>
struct Delete<Binary, false> {
    std::int32_t oid;
    std::optional<OldDataOrPrimaryKeyTupleData<Binary>> oldDataOrPrimaryKey;

    constexpr static std::size_t minBufferSize = sizeof(oid);
};

template <bool Streaming>
struct Truncate;

template <>
struct Truncate<true> {
    std::int32_t transactionId;
    std::int8_t flags;
    std::vector<std::int32_t> oids;
    constexpr static std::size_t minBufferSize =
        sizeof(transactionId) + sizeof(flags) + sizeof(std::int32_t);
};

template <>
struct Truncate<false> {
    std::int8_t flags;
    std::vector<std::int32_t> oids;

    constexpr static std::size_t minBufferSize =
        sizeof(flags) + sizeof(std::int32_t);
};

struct StreamStart {
    std::int32_t transactionId;
    std::int8_t flags;

    constexpr static std::size_t bufferSize =
        sizeof(transactionId) + sizeof(flags);
};

struct StreamStop {};

struct StreamCommit {
    std::int32_t transactionId;
    std::int8_t flags;
    std::int64_t lsn;
    std::int64_t endLsn;
    std::int64_t timestamp;

    constexpr static std::size_t bufferSize =
        sizeof(transactionId) + sizeof(flags) + sizeof(lsn) + sizeof(endLsn) +
        sizeof(timestamp);
};

template <bool IsParallel>
struct StreamAbort;

template <>
struct StreamAbort<true> {
    std::int32_t transactionId;
    std::int32_t subTransactionId;
    std::int64_t lsn;
    std::int64_t timestamp;

    constexpr static std::size_t bufferSize = sizeof(transactionId) +
                                              sizeof(subTransactionId) +
                                              sizeof(lsn) + sizeof(timestamp);
};

template <>
struct StreamAbort<false> {
    std::int32_t transactionId;
    std::int32_t subTransactionId;

    constexpr static std::size_t bufferSize =
        sizeof(transactionId) + sizeof(subTransactionId);
};

struct BeginPrepare {
    std::int64_t lsn;
    std::int64_t endLsn;
    std::int64_t timestamp;
    std::int32_t transactionId;
    std::string gid;

    constexpr static std::size_t minBufferSize = sizeof(lsn) + sizeof(endLsn) +
                                                 sizeof(timestamp) +
                                                 sizeof(transactionId) + 1;
};

struct Prepare {
    std::int8_t flags;
    std::int64_t lsn;
    std::int64_t endLsn;
    std::int64_t timestamp;
    std::int32_t transactionId;
    std::string gid;

    constexpr static std::size_t minBufferSize =
        sizeof(flags) + sizeof(lsn) + sizeof(endLsn) + sizeof(timestamp) +
        sizeof(transactionId) + 1;
};

struct CommitPrepared {
    std::int8_t flags;
    std::int64_t lsn;
    std::int64_t endLsn;
    std::int64_t timestamp;
    std::int32_t transactionId;
    std::string gid;

    constexpr static std::size_t minBufferSize =
        sizeof(flags) + sizeof(lsn) + sizeof(endLsn) + sizeof(timestamp) +
        sizeof(transactionId) + 1;
};

struct RollbackPrepared {
    std::int8_t flags;
    std::int64_t lsn;
    std::int64_t endLsn;
    std::int64_t timestamp;
    std::int64_t rollbackTimestamp;
    std::int32_t transactionId;
    std::string gid;

    constexpr static std::size_t minBufferSize =
        sizeof(flags) + sizeof(lsn) + sizeof(endLsn) + sizeof(timestamp) +
        sizeof(rollbackTimestamp) + sizeof(transactionId) + 1;
};

struct StreamPrepare {
    std::int8_t flags;
    std::int64_t lsn;
    std::int64_t endLsn;
    std::int64_t timestamp;
    std::int32_t transactionId;
    std::string gid;

    constexpr static std::size_t minBufferSize =
        sizeof(flags) + sizeof(lsn) + sizeof(endLsn) + sizeof(timestamp) +
        sizeof(transactionId) + 1;
};

enum class StreamingValue { ON, OFF, PARALLEL };

template <bool Binary, bool Messages, StreamingValue Streaming,
          bool TwoPhaseEnabled>
using Event = utils::make_variant_t<
    Begin,
    std::conditional_t<Messages, Message<Streaming != StreamingValue::OFF>,
                       void>,
    Commit, Origin, Relation<Streaming != StreamingValue::OFF>,
    Type<Streaming != StreamingValue::OFF>,
    Insert<Binary, Streaming != StreamingValue::OFF>,
    Update<Binary, Streaming != StreamingValue::OFF>,
    Delete<Binary, Streaming != StreamingValue::OFF>,
    Truncate<Streaming != StreamingValue::OFF>,
    std::conditional_t<Streaming != StreamingValue::OFF, StreamStart, void>,
    std::conditional_t<Streaming != StreamingValue::OFF, StreamStop, void>,
    std::conditional_t<Streaming != StreamingValue::OFF, StreamCommit, void>,
    std::conditional_t<Streaming != StreamingValue::OFF,
                       StreamAbort<Streaming == StreamingValue::PARALLEL>,
                       void>,
    std::conditional_t<TwoPhaseEnabled, BeginPrepare, void>,
    std::conditional_t<TwoPhaseEnabled, Prepare, void>,
    std::conditional_t<TwoPhaseEnabled, CommitPrepared, void>,
    std::conditional_t<TwoPhaseEnabled, RollbackPrepared, void>,
    std::conditional_t<TwoPhaseEnabled && Streaming != StreamingValue::OFF,
                       StreamPrepare, void>>;

enum class BaseEventType {
    BEGIN = 'B',
    COMMIT = 'C',
    ORIGIN = 'O',
    RELATION = 'R',
    TYPE = 'Y',
    INSERT = 'I',
    UPDATE = 'U',
    DELETE = 'D',
    TRUNCATE = 'T'
};

enum class MessagesEventType { MESSAGE = 'M' };

enum class StreamingEventType {
    STREAM_START = 'S',
    STREAM_STOP = 'E',
    STREAM_COMMIT = 'c',
    STREAM_ABORT = 'A',
};

enum class TwoPhaseCommitEventType {
    BEGIN_PREPARE = 'b',
    PREPARE = 'P',
    COMMIT_PREPARED = 'K',
    ROLLBACK_PREPARED = 'r',
};

enum class StreamingAndTwoPhaseCommitEventType { STREAM_PREPARE = 'p' };

template <bool Messages, bool Streaming, bool TwoPhaseEnabled>
using EventType = utils::make_variant_t<
    BaseEventType, std::conditional_t<Messages, MessagesEventType, void>,
    std::conditional_t<Streaming, StreamingEventType, void>,
    std::conditional_t<TwoPhaseEnabled, TwoPhaseCommitEventType, void>,
    std::conditional_t<Streaming && TwoPhaseEnabled,
                       StreamingAndTwoPhaseCommitEventType, void>>;

std::optional<BaseEventType> parseBaseEvenType(const char &c);
std::optional<StreamingEventType> parseStreamingEventType(const char &c);
std::optional<TwoPhaseCommitEventType> parseTwoPhaseCommitEventType(
    const char &c);

template <bool Messages, bool Streaming, bool TwoPhaseEnabled>
std::optional<EventType<Messages, Streaming, TwoPhaseEnabled>> parseEventType(
    const char &c) {
    const auto &baseEventType = parseBaseEvenType(c);
    if (baseEventType.has_value()) return baseEventType.value();
    if constexpr (Messages) {
        if (static_cast<MessagesEventType>(c) == MessagesEventType::MESSAGE)
            return MessagesEventType::MESSAGE;
    };
    if constexpr (Streaming) {
        const auto &streamingEventType = parseStreamingEventType(c);
        if (streamingEventType.has_value()) return streamingEventType.value();
    };
    if constexpr (TwoPhaseEnabled) {
        const auto &twoPhaseCommitEventType = parseTwoPhaseCommitEventType(c);
        if (twoPhaseCommitEventType.has_value())
            return twoPhaseCommitEventType.value();
    };

    if constexpr (Streaming && TwoPhaseEnabled) {
        if (static_cast<StreamingAndTwoPhaseCommitEventType>(c) ==
            StreamingAndTwoPhaseCommitEventType::STREAM_PREPARE)
            return StreamingAndTwoPhaseCommitEventType::STREAM_PREPARE;
    };
    return std::nullopt;
};

Begin parseBeginEvent(const std::span<char, Begin::bufferSize> &buffer);
Commit parseCommitEvent(const std::span<char, Commit::bufferSize> &buffer);
Origin parseOriginEvent(const std::span<char> &buffer);

std::vector<RelationColumn> parseRelationColumns(
    const std::int16_t &columnCount, const std::span<char> &buffer);

template <bool Streaming>
Relation<Streaming> parseRelationEvent(const std::span<char> &buffer);

template <bool Streaming>
Type<Streaming> parseTypeEvent(const std::span<char> &buffer);

template <bool Binary>
std::pair<TupleDataColumn<Binary>, unsigned int> parseTupleColumn(
    const std::span<char> &buffer) {
    const auto &c = buffer.front();
    switch (c) {
        case 'n':
            return { PGNull{}, 1 };
        case 'u':
            return { PGUnchangedToastedValue{}, 1 };
    };
    const auto &valueSize = utils::int32FromNetwork(buffer.subspan<1, 4>());
    const auto &valueBuffer = buffer.subspan(5, valueSize);
    if constexpr (Binary) {
        assert(c == 'b');
        return { std::vector<std::byte>(
                     reinterpret_cast<std::byte *>(valueBuffer.begin().base()),
                     reinterpret_cast<std::byte *>(valueBuffer.end().base())),
                 5 + valueSize };
    } else {
        assert(c == 't');
        return { std::string(valueBuffer.begin(), valueBuffer.end()),
                 5 + valueSize };
    };
};

template <bool Binary>
std::pair<TupleData<Binary>, unsigned int> parseTupleData(
    const std::span<char> &buffer) {
    TupleData<Binary> data;
    const auto &columnSize = utils::int16FromNetwork(buffer.subspan<0, 2>());
    unsigned int bufferPosition = 2;
    for (std::int16_t index = 0; index < columnSize; index++) {
        const auto &[column, readBytes] =
            parseTupleColumn<Binary>(buffer.subspan(bufferPosition));
        data.emplace_back(column);
        bufferPosition += readBytes;
    };
    return { data, bufferPosition };
};

template <bool Binary, bool Streaming>
Insert<Binary, Streaming> parseInsertEvent(const std::span<char> &buffer);

template <bool Binary>
Insert<Binary, true> parseInsertEvent(const std::span<char> &buffer) {
    return { .transactionId = utils::int32FromNetwork(buffer.subspan<0, 4>()),
             .oid = utils::int32FromNetwork(buffer.subspan<4, 4>()),
             .data = parseTupleData<Binary>(buffer.subspan<8>()).first };
};

template <bool Binary>
Insert<Binary, false> parseInsertEvent(const std::span<char> &buffer) {
    return { .oid = utils::int32FromNetwork(buffer.subspan<0, 4>()),
             .data = parseTupleData<Binary>(buffer.subspan<4>()).first };
};

template <bool Binary>
std::pair<std::optional<OldDataOrPrimaryKeyTupleData<Binary>>, unsigned int>
parseOldDataOrPrimaryKey(const std::span<char> &buffer) {
    if (buffer.size() == 0) return { std::nullopt, 0 };
    const auto &c = buffer.front();
    switch (c) {
        case 'K': {
            const auto &[tupleData, readBytes] =
                parseTupleData<Binary>(buffer.subspan<1>());
            return { PrimaryKeyTupleData<Binary>(tupleData), readBytes + 1 };
        }
        case 'O':
            const auto &[tupleData, readBytes] =
                parseTupleData<Binary>(buffer.subspan<1>());
            return { OldTupleData<Binary>(tupleData), readBytes + 1 };
    };
    return { std::nullopt, 0 };
};

template <bool Binary, bool Streaming>
struct ParseUpdateEvent {
    static Update<Binary, Streaming> parseUpdateEvent(
        const std::span<char> &buffer);
};

template <bool Binary>
struct ParseUpdateEvent<Binary, true> {
    static Update<Binary, true> parseUpdateEvent(
        const std::span<char> &buffer) {
        const auto &transactionId =
            utils::int32FromNetwork(buffer.subspan<0, 4>());
        const auto &oid = utils::int32FromNetwork(buffer.subspan<4, 4>());
        const auto &[oldDataOrPrimaryKey, readBytes] =
            parseOldDataOrPrimaryKey<Binary>(buffer.subspan<8>());
        return {
            .transactionId = transactionId,
            .oid = oid,
            .oldDataOrPrimaryKey = oldDataOrPrimaryKey,
            .data = parseTupleData<Binary>(buffer.subspan(8 + readBytes)).first
        };
    };
};

template <bool Binary>
struct ParseUpdateEvent<Binary, false> {
    static Update<Binary, false> parseUpdateEvent(
        const std::span<char> &buffer) {
        const auto &oid = utils::int32FromNetwork(buffer.subspan<0, 4>());
        const auto &[oldDataOrPrimaryKey, readBytes] =
            parseOldDataOrPrimaryKey<Binary>(buffer.subspan<4>());
        return {
            .oid = oid,
            .oldDataOrPrimaryKey = oldDataOrPrimaryKey,
            .data = parseTupleData<Binary>(buffer.subspan(4 + readBytes)).first
        };
    };
};

template <bool Binary, bool Streaming>
Delete<Binary, Streaming> parseDeleteEvent(const std::span<char> &buffer);

template <bool Binary>
Delete<Binary, true> parseDeleteEvent(const std::span<char> &buffer) {
    const auto &transactionId = utils::int32FromNetwork(buffer.subspan<0, 4>());
    const auto &oid = utils::int32FromNetwork(buffer.subspan<4, 4>());
    return { .transactionId = transactionId,
             .oid = oid,
             .oldDataOrPrimaryKey =
                 parseOldDataOrPrimaryKey<Binary>(buffer.subspan<8>()).first };
};

template <bool Binary>
Delete<Binary, false> parseDeleteEvent(const std::span<char> &buffer) {
    const auto &oid = utils::int32FromNetwork(buffer.subspan<0, 4>());
    return {
        .oid = oid,
        .oldDataOrPrimaryKey =
            parseOldDataOrPrimaryKey<Binary>(buffer.subspan<4>()).first,
    };
};

template <bool Streaming>
Truncate<Streaming> parseTruncateEvent(const std::span<char> &buffer);

template <typename T>
concept HasBufferSize = requires(T a) {
    { T::bufferSize } -> std::convertible_to<std::size_t>;
};

template <HasBufferSize T>
std::expected<T, std::string> parseStaticSizeEvent(
    const std::span<char> &buffer,
    const std::function<T(const std::span<char, T::bufferSize> &)>
        &parseCallback) {
    if (buffer.size() != T::bufferSize) {
        return std::unexpected(std::format("{} event buffer size must be {}",
                                           typeid(T).name(), T::bufferSize));
    };
    return parseCallback(buffer.subspan<0, T::bufferSize>());
};

template <typename T>
concept HasMinBufferSize = requires(T a) {
    { T::minBufferSize } -> std::convertible_to<std::size_t>;
};

template <HasMinBufferSize T>
std::expected<T, std::string> parseDynamicSizeEvent(
    const std::span<char> &buffer,
    const std::function<T(const std::span<char> &)> &parseCallback) {
    if (buffer.size() < T::minBufferSize) {
        return std::unexpected(
            std::format("{} event buffer size must be gte {}", typeid(T).name(),
                        T::minBufferSize));
    };
    return parseCallback(buffer.subspan<0>());
};

template <bool Binary, bool Messages, StreamingValue Streaming,
          bool TwoPhaseEnabled>
std::expected<Event<Binary, Messages, Streaming, TwoPhaseEnabled>, std::string>
parseBaseEvent(const BaseEventType &eventType, const std::span<char> &buffer) {
    constexpr auto IsStreaming = Streaming != StreamingValue::OFF;
    switch (eventType) {
        case BaseEventType::BEGIN:
            return parseStaticSizeEvent<Begin>(buffer, parseBeginEvent);
        case BaseEventType::COMMIT:
            return parseStaticSizeEvent<Commit>(buffer, parseCommitEvent);
        case BaseEventType::ORIGIN:
            return parseDynamicSizeEvent<Origin>(buffer, parseOriginEvent);
        case BaseEventType::RELATION:
            return parseDynamicSizeEvent<Relation<IsStreaming>>(
                buffer, parseRelationEvent<IsStreaming>);
        case BaseEventType::TYPE:
            return parseDynamicSizeEvent<Type<IsStreaming>>(
                buffer, parseTypeEvent<IsStreaming>);
        case BaseEventType::INSERT:
            return parseDynamicSizeEvent<Insert<Binary, IsStreaming>>(
                buffer, parseInsertEvent<Binary, IsStreaming>);
        case BaseEventType::UPDATE:
            return parseDynamicSizeEvent<Update<Binary, IsStreaming>>(
                buffer, ParseUpdateEvent<Binary, IsStreaming>::parseUpdateEvent);
        case BaseEventType::DELETE:
            return parseDynamicSizeEvent<Delete<Binary, IsStreaming>>(
                buffer, parseDeleteEvent<Binary, IsStreaming>);
        case BaseEventType::TRUNCATE:
            return parseDynamicSizeEvent<Truncate<IsStreaming>>(
                buffer, parseTruncateEvent<IsStreaming>);
    };
};

template <bool Streaming>
Message<Streaming> parseMessageEvent(const std::span<char> &buffer);

StreamStart parseStreamStart(
    const std::span<char, StreamStart::bufferSize> &buffer);
StreamCommit parseStreamCommit(
    const std::span<char, StreamCommit::bufferSize> &buffer);

template <bool IsParallel>
StreamAbort<IsParallel> parseStreamAbort(
    const std::span<char, StreamAbort<IsParallel>::bufferSize> &buffer);

template <bool IsParallel>
std::expected<std::variant<StreamStart, StreamStop, StreamCommit,
                           StreamAbort<IsParallel>>,
              std::string>
parseStreamingEvent(const StreamingEventType &eventType,
                    const std::span<char> &buffer) {
    switch (eventType) {
        case StreamingEventType::STREAM_START:
            return parseStaticSizeEvent<StreamStart>(buffer, parseStreamStart);
        case StreamingEventType::STREAM_STOP:
            return StreamStop{};
        case StreamingEventType::STREAM_COMMIT:
            return parseStaticSizeEvent<StreamCommit>(buffer,
                                                      parseStreamCommit);
        case StreamingEventType::STREAM_ABORT:
            return parseStaticSizeEvent<StreamAbort<IsParallel>>(
                buffer, parseStreamAbort<IsParallel>);
    };
};

std::expected<
    std::variant<BeginPrepare, Prepare, CommitPrepared, RollbackPrepared>,
    std::string>
parseTwoPhaseCommitEvent(const TwoPhaseCommitEventType &eventType,
                         const std::span<char> &buffer);

StreamPrepare parseStreamPrepare(const std::span<char> &buffer);

template <bool Binary, bool Messages, StreamingValue Streaming,
          bool TwoPhaseEnabled>
std::expected<Event<Binary, Messages, Streaming, TwoPhaseEnabled>, std::string>
parseEventByType(const EventType<Messages, Streaming != StreamingValue::OFF,
                                 TwoPhaseEnabled> &eventType,
                 const std::span<char> &buffer) {
    if (std::holds_alternative<BaseEventType>(eventType)) {
        const auto &baseEventType = std::get<BaseEventType>(eventType);
        return parseBaseEvent<Binary, Messages, Streaming, TwoPhaseEnabled>(
            baseEventType, buffer);
    };
    if constexpr (Messages) {
        if (std::holds_alternative<MessagesEventType>(eventType)) {
            return parseMessageEvent<Streaming>(buffer);
        };
    };
    constexpr auto IsStreaming = Streaming != StreamingValue::OFF;
    if constexpr (IsStreaming) {
        constexpr auto IsParallel = Streaming == StreamingValue::PARALLEL;
        if (std::holds_alternative<StreamingEventType>(eventType)) {
            const auto &streamingEventType =
                std::get<StreamingEventType>(eventType);
            return parseStreamingEvent<IsParallel>(streamingEventType, buffer);
        };
    };
    if constexpr (TwoPhaseEnabled) {
        if (std::holds_alternative<TwoPhaseCommitEventType>(eventType)) {
            const auto &twoPhaseCommitEventType =
                std::get<TwoPhaseCommitEventType>(eventType);
            return parseTwoPhaseCommitEvent(eventType, buffer);
        };
    };
    if constexpr (TwoPhaseEnabled && IsStreaming) {
        if (std::holds_alternative<StreamingAndTwoPhaseCommitEventType>(
                eventType)) {
            return parseDynamicSizeEvent<StreamPrepare>(buffer,
                                                        parseStreamPrepare);
        };
    };
    throw std::runtime_error("No event type was matched");
};

template <bool Binary, bool Messages, StreamingValue Streaming,
          bool TwoPhaseEnabled>
std::expected<Event<Binary, Messages, Streaming, TwoPhaseEnabled>, std::string>
parseEvent(const std::span<char> &buffer) {
    assert(buffer.size() > 0);
    const auto &eventTypeOptional = parseEventType < Messages,
               Streaming != StreamingValue::OFF, TwoPhaseEnabled > (buffer[0]);
    if (!eventTypeOptional.has_value()) {
        return std::unexpected(std::format("Unexpected type: '{}'", buffer[0]));
    };
    const auto &eventType = eventTypeOptional.value();
    return parseEventByType<Binary, Messages, Streaming, TwoPhaseEnabled>(
        eventType, buffer.subspan(1));
};
};  // namespace PGREPLICATION_NAMESPACE::pgoutput
