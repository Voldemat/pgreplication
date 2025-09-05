#pragma once

#include <cstddef>
#include <cstdint>
#include <format>
#include <span>

#include "./tuple_data.hpp"
#include "pgreplication/pgoutput/options.hpp"
#include "pgreplication/utils.hpp"

namespace PGREPLICATION_NAMESPACE::pgoutput::events {
template <BinaryValue Binary, StreamingEnabledValue Streaming>
struct Insert;

template <BinaryValue Binary>
struct Insert<Binary, StreamingEnabledValue::ON> {
    std::int32_t transactionId;
    std::int32_t oid;
    TupleData<Binary> data;

    constexpr static std::size_t minBufferSize =
        sizeof(transactionId) + sizeof(oid) + sizeof(std::int16_t);
    using input_buffer = std::span<char>;

    constexpr static Insert<Binary, StreamingEnabledValue::ON> fromBuffer(
        const input_buffer &buffer) {
        return {
            .transactionId = ::PGREPLICATION_NAMESPACE::utils::int32FromNetwork(
                buffer.subspan<0, 4>()),
            .oid = ::PGREPLICATION_NAMESPACE::utils::int32FromNetwork(
                buffer.subspan<4, 4>()),
            .data =
                parseTupleData<Binary>(buffer.subspan<8 + sizeof('N')>()).first
        };
    };
};

template <BinaryValue Binary>
struct Insert<Binary, StreamingEnabledValue::OFF> {
    std::int32_t oid;
    TupleData<Binary> data;

    constexpr static std::size_t minBufferSize =
        sizeof(oid) + sizeof(std::int16_t);
    using input_buffer = std::span<char>;

    constexpr static Insert<Binary, StreamingEnabledValue::OFF> fromBuffer(
        const input_buffer &buffer) {
        return {
            .oid = ::PGREPLICATION_NAMESPACE::utils::int32FromNetwork(
                buffer.subspan<0, 4>()),
            .data =
                parseTupleData<Binary>(buffer.subspan<4 + sizeof('N')>()).first
        };
    };
};
};  // namespace PGREPLICATION_NAMESPACE::pgoutput::events

namespace std {
template <PGREPLICATION_NAMESPACE::pgoutput::BinaryValue Binary>
struct std::formatter<PGREPLICATION_NAMESPACE::pgoutput::events::Insert<
    Binary, PGREPLICATION_NAMESPACE::pgoutput::StreamingEnabledValue::ON>> {
    template <typename ParseContext>
    constexpr auto parse(ParseContext &ctx) {
        return ctx.begin();
    }

    template <typename FormatContext>
    auto format(
        const PGREPLICATION_NAMESPACE::pgoutput::events::Insert<
            Binary,
            PGREPLICATION_NAMESPACE::pgoutput::StreamingEnabledValue::ON>
            &record,
        FormatContext &ctx) const {
        return std::format_to(ctx.out(),
                              "Insert(transactionId: {}, oid: {}, data: {})",
                              record.transactionId, record.oid, record.data);
    }
};

template <PGREPLICATION_NAMESPACE::pgoutput::BinaryValue Binary>
struct formatter<PGREPLICATION_NAMESPACE::pgoutput::events::Insert<
    Binary, PGREPLICATION_NAMESPACE::pgoutput::StreamingEnabledValue::OFF>> {
    template <typename ParseContext>
    constexpr auto parse(ParseContext &ctx) {
        return ctx.begin();
    }

    template <typename FormatContext>
    auto format(
        const PGREPLICATION_NAMESPACE::pgoutput::events::Insert<
            Binary,
            PGREPLICATION_NAMESPACE::pgoutput::StreamingEnabledValue::OFF>
            &record,
        FormatContext &ctx) const {
        return format_to(ctx.out(), "Insert(oid: {}, data: {})",
                              record.oid, record.data);
    }
};
};  // namespace std
