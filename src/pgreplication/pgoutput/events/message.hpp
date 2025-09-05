#pragma once

#include <cstddef>
#include <cstdint>
#include <format>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "pgreplication/pgoutput/options.hpp"

namespace PGREPLICATION_NAMESPACE::pgoutput::events {
enum class MessagesEventType { MESSAGE = 'M' };

template <StreamingEnabledValue Streaming>
struct Message;

template <>
struct Message<StreamingEnabledValue::ON> {
    std::int32_t transactionId;
    std::int8_t flags;
    std::int64_t lsn;
    std::string prefix;
    std::vector<std::byte> content;

    constexpr static const std::size_t minBufferSize =
        sizeof(transactionId) + sizeof(flags) + sizeof(lsn) + sizeof(prefix) +
        sizeof(std::int32_t);
    using input_buffer = std::span<char>;

    static Message<StreamingEnabledValue::ON> fromBuffer(
        const input_buffer &buffer);
};

template <>
struct Message<StreamingEnabledValue::OFF> {
    std::int8_t flags;
    std::int64_t lsn;
    std::string prefix;
    std::vector<std::byte> content;

    constexpr static const std::size_t minBufferSize =
        sizeof(flags) + sizeof(lsn) + sizeof(prefix) + sizeof(std::int32_t);

    using input_buffer = std::span<char>;

    static Message<StreamingEnabledValue::OFF> fromBuffer(
        const input_buffer &buffer);
};
};  // namespace PGREPLICATION_NAMESPACE::pgoutput::events

namespace std {
template <>
struct formatter<PGREPLICATION_NAMESPACE::pgoutput::events::Message<
    PGREPLICATION_NAMESPACE::pgoutput::StreamingEnabledValue::ON>> {
    template <typename ParseContext>
    constexpr auto parse(ParseContext &ctx) {
        return ctx.begin();
    }

    template <typename FormatContext>
    auto format(
        const PGREPLICATION_NAMESPACE::pgoutput::events::Message<
            PGREPLICATION_NAMESPACE::pgoutput::StreamingEnabledValue::ON>
            &record,
        FormatContext &ctx) const {
        return format_to(
            ctx.out(),
            "Message(transactionId: {}, flags: {}, "
            "lsn: {}, prefix: {}, content: {})",
            record.transactionId, record.flags, record.lsn, record.prefix,
            string_view(
                reinterpret_cast<const char *>(record.content.begin().base()),
                reinterpret_cast<const char *>(record.content.end().base())));
    }
};

template <>
struct formatter<PGREPLICATION_NAMESPACE::pgoutput::events::Message<
    PGREPLICATION_NAMESPACE::pgoutput::StreamingEnabledValue::OFF>> {
    template <typename ParseContext>
    constexpr auto parse(ParseContext &ctx) {
        return ctx.begin();
    }

    template <typename FormatContext>
    auto format(
        const PGREPLICATION_NAMESPACE::pgoutput::events::Message<
            PGREPLICATION_NAMESPACE::pgoutput::StreamingEnabledValue::OFF>
            &record,
        FormatContext &ctx) const {
        return format_to(
            ctx.out(), "Message(flags: {}, lsn: {}, prefix: {}, content: {})",
            record.flags, record.lsn, record.prefix,
            std::string_view(
                reinterpret_cast<const char *>(record.content.begin().base()),
                reinterpret_cast<const char *>(record.content.end().base())));
    }
};
};  // namespace std
