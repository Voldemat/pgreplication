#pragma once

#include <cstddef>
#include <cstdint>
#include <expected>
#include <format>
#include <optional>
#include <span>
#include <string>
#include <variant>

#include "../options.hpp"
#include "./utils.hpp"

namespace PGREPLICATION_NAMESPACE::pgoutput::events {

enum class StreamingEventType {
    STREAM_START = 'S',
    STREAM_STOP = 'E',
    STREAM_COMMIT = 'c',
    STREAM_ABORT = 'A',
};

std::optional<StreamingEventType> parseStreamingEventType(const char &c);

struct StreamStart {
    std::int32_t transactionId;
    std::int8_t flags;

    constexpr static std::size_t bufferSize =
        sizeof(transactionId) + sizeof(flags);
    using input_buffer = std::span<char, bufferSize>;

    static StreamStart fromBuffer(const input_buffer &buffer);
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
    using input_buffer = std::span<char, bufferSize>;

    static StreamCommit fromBuffer(const input_buffer &buffer);
};

template <StreamingValue Streaming>
struct StreamAbort;

template <>
struct StreamAbort<StreamingValue::PARALLEL> {
    std::int32_t transactionId;
    std::int32_t subTransactionId;
    std::int64_t lsn;
    std::int64_t timestamp;

    constexpr static std::size_t bufferSize = sizeof(transactionId) +
                                              sizeof(subTransactionId) +
                                              sizeof(lsn) + sizeof(timestamp);
    using input_buffer = std::span<char, bufferSize>;

    static StreamAbort<StreamingValue::PARALLEL> fromBuffer(
        const input_buffer &buffer);
};

template <>
struct StreamAbort<StreamingValue::ON> {
    std::int32_t transactionId;
    std::int32_t subTransactionId;

    constexpr static std::size_t bufferSize =
        sizeof(transactionId) + sizeof(subTransactionId);
    using input_buffer = std::span<char, bufferSize>;

    static StreamAbort<StreamingValue::ON> fromBuffer(
        const input_buffer &buffer);
};

template <StreamingValue Streaming>
std::expected<
    std::variant<StreamStart, StreamStop, StreamCommit, StreamAbort<Streaming>>,
    std::string>
parseStreamingEvent(const StreamingEventType &eventType,
                    const std::span<char> &buffer) {
    switch (eventType) {
        case StreamingEventType::STREAM_START:
            return utils::parseStaticSizeEvent<StreamStart>(buffer);
        case StreamingEventType::STREAM_STOP:
            return StreamStop{};
        case StreamingEventType::STREAM_COMMIT:
            return utils::parseStaticSizeEvent<StreamCommit>(buffer);
        case StreamingEventType::STREAM_ABORT:
            return utils::parseStaticSizeEvent<StreamAbort<Streaming>>(buffer);
    };
};

};  // namespace PGREPLICATION_NAMESPACE::pgoutput::events

namespace std {
template <>
struct std::formatter<PGREPLICATION_NAMESPACE::pgoutput::events::StreamStart> {
    template <typename ParseContext>
    constexpr auto parse(ParseContext &ctx) {
        return ctx.begin();
    }

    template <typename FormatContext>
    auto format(
        const PGREPLICATION_NAMESPACE::pgoutput::events::StreamStart &record,
        FormatContext &ctx) const {
        return std::format_to(ctx.out(),
                              "StreamStart(transactionId: {}, flags: {})",
                              record.transactionId, record.flags);
    }
};

template <>
struct std::formatter<PGREPLICATION_NAMESPACE::pgoutput::events::StreamStop> {
    template <typename ParseContext>
    constexpr auto parse(ParseContext &ctx) {
        return ctx.begin();
    }

    template <typename FormatContext>
    auto format(
        const PGREPLICATION_NAMESPACE::pgoutput::events::StreamStop &record,
        FormatContext &ctx) const {
        return std::format_to(ctx.out(), "StreamStop()");
    }
};

template <>
struct std::formatter<PGREPLICATION_NAMESPACE::pgoutput::events::StreamCommit> {
    template <typename ParseContext>
    constexpr auto parse(ParseContext &ctx) {
        return ctx.begin();
    }

    template <typename FormatContext>
    auto format(
        const PGREPLICATION_NAMESPACE::pgoutput::events::StreamCommit &record,
        FormatContext &ctx) const {
        return std::format_to(ctx.out(),
                              "StreamCommit(transactionId: {}, flags: {}, lsn: "
                              "{}, endLsn: {}, timestamp: {})",
                              record.transactionId, record.flags, record.lsn,
                              record.endLsn, record.timestamp);
    }
};

template <>
struct std::formatter<PGREPLICATION_NAMESPACE::pgoutput::events::StreamAbort<
    PGREPLICATION_NAMESPACE::pgoutput::StreamingValue::PARALLEL>> {
    template <typename ParseContext>
    constexpr auto parse(ParseContext &ctx) {
        return ctx.begin();
    }

    template <typename FormatContext>
    auto format(const PGREPLICATION_NAMESPACE::pgoutput::events::StreamAbort<
                    PGREPLICATION_NAMESPACE::pgoutput::StreamingValue::PARALLEL>
                    &record,
                FormatContext &ctx) const {
        return std::format_to(ctx.out(),
                              "StreamAbort(transactionId: {}, "
                              "subTransactionId: {}, lsn: {}, timestamp: {})",
                              record.transactionId, record.subTransactionId,
                              record.lsn, record.timestamp);
    }
};

template <>
struct std::formatter<PGREPLICATION_NAMESPACE::pgoutput::events::StreamAbort<
    PGREPLICATION_NAMESPACE::pgoutput::StreamingValue::ON>> {
    template <typename ParseContext>
    constexpr auto parse(ParseContext &ctx) {
        return ctx.begin();
    }

    template <typename FormatContext>
    auto format(
        const PGREPLICATION_NAMESPACE::pgoutput::events::StreamAbort<
            PGREPLICATION_NAMESPACE::pgoutput::StreamingValue::ON> &record,
        FormatContext &ctx) const {
        return std::format_to(ctx.out(),
                              "StreamAbort(transactionId: {}, "
                              "subTransactionId: {})",
                              record.transactionId, record.subTransactionId);
    }
};
};  // namespace std
