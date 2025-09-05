#pragma once

#include <cstddef>
#include <cstdint>
#include <format>
#include <span>
#include <string>

#include "pgreplication/pgoutput/options.hpp"

namespace PGREPLICATION_NAMESPACE::pgoutput::events {
template <StreamingEnabledValue Streaming>
struct Type;

template <>
struct Type<StreamingEnabledValue::ON> {
    std::int32_t transactionId;
    std::int32_t oid;
    std::string typeNamespace;
    std::string name;

    constexpr static std::size_t minBufferSize =
        sizeof(transactionId) + sizeof(oid) + 1 + 1;
    using input_buffer = std::span<char>;

    static Type<StreamingEnabledValue::ON> fromBuffer(
        const input_buffer &buffer);
};

template <>
struct Type<StreamingEnabledValue::OFF> {
    std::int32_t oid;
    std::string typeNamespace;
    std::string name;

    constexpr static std::size_t minBufferSize = sizeof(oid) + 1 + 1;
    using input_buffer = std::span<char>;

    static Type<StreamingEnabledValue::OFF> fromBuffer(
        const input_buffer &buffer);
};

};  // namespace PGREPLICATION_NAMESPACE::pgoutput::events

namespace std {
template <>
struct formatter<PGREPLICATION_NAMESPACE::pgoutput::events::Type<
    PGREPLICATION_NAMESPACE::pgoutput::StreamingEnabledValue::ON>> {
    template <typename ParseContext>
    constexpr auto parse(ParseContext &ctx) {
        return ctx.begin();
    }

    template <typename FormatContext>
    auto format(
        const PGREPLICATION_NAMESPACE::pgoutput::events::Type<
            PGREPLICATION_NAMESPACE::pgoutput::StreamingEnabledValue::ON>
            &record,
        FormatContext &ctx) const {
        return format_to(
            ctx.out(),
            "Type(transactionId: {}, oid: {}, namespace: {}, name: {})",
            record.transactionId, record.oid, record.typeNamespace,
            record.name);
    }
};

template <>
struct formatter<PGREPLICATION_NAMESPACE::pgoutput::events::Type<
    PGREPLICATION_NAMESPACE::pgoutput::StreamingEnabledValue::OFF>> {
    template <typename ParseContext>
    constexpr auto parse(ParseContext &ctx) {
        return ctx.begin();
    }

    template <typename FormatContext>
    auto format(
        const PGREPLICATION_NAMESPACE::pgoutput::events::Type<
            PGREPLICATION_NAMESPACE::pgoutput::StreamingEnabledValue::OFF>
            &record,
        FormatContext &ctx) const {
        return format_to(ctx.out(),
                              "Type(oid: {}, namespace: {}, name: {})",
                              record.oid, record.typeNamespace, record.name);
    }
};
};  // namespace std
