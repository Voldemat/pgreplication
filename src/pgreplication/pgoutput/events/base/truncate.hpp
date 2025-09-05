#pragma once

#include <cstddef>
#include <cstdint>
#include <format>
#include <span>
#include <vector>

#include "pgreplication/pgoutput/options.hpp"

namespace PGREPLICATION_NAMESPACE::pgoutput::events {
template <StreamingEnabledValue Streaming>
struct Truncate;

template <>
struct Truncate<StreamingEnabledValue::ON> {
    std::int32_t transactionId;
    std::int8_t flags;
    std::vector<std::int32_t> oids;

    constexpr static std::size_t minBufferSize =
        sizeof(transactionId) + sizeof(flags) + sizeof(std::int32_t);
    using input_buffer = std::span<char>;

    static Truncate<StreamingEnabledValue::ON> fromBuffer(
        const input_buffer &buffer);
};

template <>
struct Truncate<StreamingEnabledValue::OFF> {
    std::int8_t flags;
    std::vector<std::int32_t> oids;

    constexpr static std::size_t minBufferSize =
        sizeof(flags) + sizeof(std::int32_t);
    using input_buffer = std::span<char>;

    static Truncate<StreamingEnabledValue::OFF> fromBuffer(
        const input_buffer &buffer);
};
};  // namespace PGREPLICATION_NAMESPACE::pgoutput::events

namespace std {
template <>
struct formatter<PGREPLICATION_NAMESPACE::pgoutput::events::Truncate<
    PGREPLICATION_NAMESPACE::pgoutput::StreamingEnabledValue::ON>> {
    template <typename ParseContext>
    constexpr auto parse(ParseContext &ctx) {
        return ctx.begin();
    }

    template <typename FormatContext>
    auto format(
        const PGREPLICATION_NAMESPACE::pgoutput::events::Truncate<
            PGREPLICATION_NAMESPACE::pgoutput::StreamingEnabledValue::ON>
            &record,
        FormatContext &ctx) const {
        return format_to(ctx.out(),
                              "Truncate(transactionId: "
                              "{}, flags: {}, oids: {})",
                              record.transactionId, record.flags, record.oids);
    }
};

template <>
struct formatter<PGREPLICATION_NAMESPACE::pgoutput::events::Truncate<
    PGREPLICATION_NAMESPACE::pgoutput::StreamingEnabledValue::OFF>> {
    template <typename ParseContext>
    constexpr auto parse(ParseContext &ctx) {
        return ctx.begin();
    }

    template <typename FormatContext>
    auto format(
        const PGREPLICATION_NAMESPACE::pgoutput::events::Truncate<
            PGREPLICATION_NAMESPACE::pgoutput::StreamingEnabledValue::OFF>
            &record,
        FormatContext &ctx) const {
        return format_to(ctx.out(), "Truncate(flags: {}, oids: {})",
                         record.flags, record.oids);
    }
};
};  // namespace std
