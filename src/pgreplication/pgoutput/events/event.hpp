#pragma once

#include <cassert>
#include <expected>
#include <format>
#include <optional>
#include <span>
#include <string>
#include <type_traits>

#include "../options.hpp"
#include "./base/begin.hpp"
#include "./base/commit.hpp"
#include "./base/delete.hpp"
#include "./base/event.hpp"
#include "./base/insert.hpp"
#include "./base/relation.hpp"
#include "./base/truncate.hpp"
#include "./base/type.hpp"
#include "./base/update.hpp"
#include "./message.hpp"
#include "./origin.hpp"
#include "./stream.hpp"
#include "./stream_and_twophase.hpp"
#include "./twophase.hpp"
#include "./utils.hpp"
#include "pgreplication/utils.hpp"

namespace PGREPLICATION_NAMESPACE::pgoutput::events {
template <BinaryValue Binary, MessagesValue Messages, StreamingValue Streaming,
          TwoPhaseValue TwoPhase, OriginValue OriginConf>
struct EventStruct {
    constexpr static auto StreamingEnabled =
        streamingValueToStreamingEnabledValue(Streaming);
    using Event = ::PGREPLICATION_NAMESPACE::utils::make_variant_t<
        Begin,
        std::conditional_t<Messages == MessagesValue::ON,
                           Message<StreamingEnabled>, void>,
        Commit, std::conditional_t<OriginConf == OriginValue::ON, Origin, void>,
        Relation<StreamingEnabled>, Type<StreamingEnabled>,
        Insert<Binary, StreamingEnabled>, Update<Binary, StreamingEnabled>,
        Delete<Binary, StreamingEnabled>, Truncate<StreamingEnabled>,
        std::conditional_t<StreamingEnabled == StreamingEnabledValue::ON,
                           StreamStart, void>,
        std::conditional_t<StreamingEnabled == StreamingEnabledValue::ON,
                           StreamStop, void>,
        std::conditional_t<StreamingEnabled == StreamingEnabledValue::ON,
                           StreamCommit, void>,
        std::conditional_t<StreamingEnabled == StreamingEnabledValue::ON,
                           StreamAbort<Streaming>, void>,
        std::conditional_t<TwoPhase == TwoPhaseValue::ON, BeginPrepare, void>,
        std::conditional_t<TwoPhase == TwoPhaseValue::ON, Prepare, void>,
        std::conditional_t<TwoPhase == TwoPhaseValue::ON, CommitPrepared, void>,
        std::conditional_t<TwoPhase == TwoPhaseValue::ON, RollbackPrepared,
                           void>,
        std::conditional_t<TwoPhase == TwoPhaseValue::ON &&
                               StreamingEnabled == StreamingEnabledValue::ON,
                           StreamPrepare, void>>;
};

template <BinaryValue Binary, MessagesValue Messages, StreamingValue Streaming,
          TwoPhaseValue TwoPhase, OriginValue OriginConf>
using Event =
    EventStruct<Binary, Messages, Streaming, TwoPhase, OriginConf>::Event;

template <MessagesValue Messages, StreamingEnabledValue StreamingEnabled,
          TwoPhaseValue TwoPhase, OriginValue OriginConf>
using EventType = ::PGREPLICATION_NAMESPACE::utils::make_variant_t<
    BaseEventType,
    std::conditional_t<Messages == MessagesValue::ON, MessagesEventType, void>,
    std::conditional_t<OriginConf == OriginValue::ON, OriginEventType, void>,
    std::conditional_t<StreamingEnabled == StreamingEnabledValue::ON,
                       StreamingEventType, void>,
    std::conditional_t<TwoPhase == TwoPhaseValue::ON, TwoPhaseCommitEventType,
                       void>,
    std::conditional_t<StreamingEnabled == StreamingEnabledValue::ON &&
                           TwoPhase == TwoPhaseValue::ON,
                       StreamingAndTwoPhaseCommitEventType, void>>;

template <MessagesValue Messages, StreamingEnabledValue StreamingEnabled,
          TwoPhaseValue TwoPhase, OriginValue OriginConf>
std::optional<EventType<Messages, StreamingEnabled, TwoPhase, OriginConf>>
parseEventType(const char &c) {
    const auto &baseEventType = parseBaseEvenType(c);
    if (baseEventType.has_value()) return baseEventType.value();
    if constexpr (Messages == MessagesValue::ON) {
        if (static_cast<MessagesEventType>(c) == MessagesEventType::MESSAGE)
            return MessagesEventType::MESSAGE;
    };
    if constexpr (OriginConf == OriginValue::ON) {
        if (static_cast<OriginEventType>(c) == OriginEventType::ORIGIN) {
            return OriginEventType::ORIGIN;
        };
    };
    if constexpr (StreamingEnabled == StreamingEnabledValue::ON) {
        const auto &streamingEventType = parseStreamingEventType(c);
        if (streamingEventType.has_value()) return streamingEventType.value();
    };
    if constexpr (TwoPhase == TwoPhaseValue::ON) {
        const auto &twoPhaseCommitEventType = parseTwoPhaseCommitEventType(c);
        if (twoPhaseCommitEventType.has_value())
            return twoPhaseCommitEventType.value();
    };

    if constexpr (StreamingEnabled == StreamingEnabledValue::ON &&
                  TwoPhase == TwoPhaseValue::ON) {
        if (static_cast<StreamingAndTwoPhaseCommitEventType>(c) ==
            StreamingAndTwoPhaseCommitEventType::STREAM_PREPARE)
            return StreamingAndTwoPhaseCommitEventType::STREAM_PREPARE;
    };
    return std::nullopt;
};

template <BinaryValue Binary, MessagesValue Messages, StreamingValue Streaming,
          TwoPhaseValue TwoPhase, OriginValue OriginConf>
std::expected<Event<Binary, Messages, Streaming, TwoPhase, OriginConf>,
              std::string>
parseEventByType(
    const EventType<Messages, streamingValueToStreamingEnabledValue(Streaming),
                    TwoPhase, OriginConf> &eventType,
    const std::span<char> &buffer) {
    constexpr auto StreamingEnabled =
        streamingValueToStreamingEnabledValue(Streaming);
    if (std::holds_alternative<BaseEventType>(eventType)) {
        const auto &baseEventType = std::get<BaseEventType>(eventType);
        return parseBaseEvent<Binary, StreamingEnabled>(baseEventType, buffer)
            .transform(
                [](const auto &event) -> Event<Binary, Messages, Streaming,
                                               TwoPhase, OriginConf> {
                    return std::visit([](auto &&arg) { return arg; }, event);
                });
    };
    if constexpr (Messages == MessagesValue::ON) {
        if (std::holds_alternative<MessagesEventType>(eventType)) {
            return Message<StreamingEnabled>::fromBuffer(buffer);
        };
    };
    if constexpr (OriginConf == OriginValue::ON) {
        if (std::holds_alternative<OriginEventType>(eventType)) {
            return Origin::fromBuffer(buffer);
        };
    };
    if constexpr (Streaming != StreamingValue::OFF) {
        if (std::holds_alternative<StreamingEventType>(eventType)) {
            const auto &streamingEventType =
                std::get<StreamingEventType>(eventType);
            return parseStreamingEvent<Streaming>(streamingEventType, buffer)
                .transform([](const auto &event) {
                    return std::visit(
                        [](auto &&arg) -> Event<Binary, Messages, Streaming,
                                                TwoPhase, OriginConf> {
                            return arg;
                        },
                        event);
                });
        };
    };
    if constexpr (TwoPhase == TwoPhaseValue::ON) {
        if (std::holds_alternative<TwoPhaseCommitEventType>(eventType)) {
            const auto &twoPhaseCommitEventType =
                std::get<TwoPhaseCommitEventType>(eventType);
            return parseTwoPhaseCommitEvent(eventType, buffer);
        };
    };
    if constexpr (TwoPhase == TwoPhaseValue::ON &&
                  Streaming != StreamingValue::OFF) {
        if (std::holds_alternative<StreamingAndTwoPhaseCommitEventType>(
                eventType)) {
            return utils::parseDynamicSizeEvent<StreamPrepare>(buffer);
        };
    };
    return std::unexpected("No event type was matched");
};

template <BinaryValue Binary, MessagesValue Messages, StreamingValue Streaming,
          TwoPhaseValue TwoPhase, OriginValue OriginConf>
std::expected<Event<Binary, Messages, Streaming, TwoPhase, OriginConf>,
              std::string>
parseEvent(const std::span<char> &buffer) {
    assert(buffer.size() > 0);
    const auto &eventTypeOptional =
        parseEventType<Messages,
                       streamingValueToStreamingEnabledValue(Streaming),
                       TwoPhase, OriginConf>(buffer[0]);
    if (!eventTypeOptional.has_value()) {
        return std::unexpected(std::format("Unexpected type: '{}'", buffer[0]));
    };
    const auto &eventType = eventTypeOptional.value();
    return parseEventByType<Binary, Messages, Streaming, TwoPhase, OriginConf>(
        eventType, buffer.subspan(1));
};

};  // namespace PGREPLICATION_NAMESPACE::pgoutput::events
