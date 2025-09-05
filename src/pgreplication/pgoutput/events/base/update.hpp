#pragma once

#include <cstddef>
#include <cstdint>
#include <format>
#include <optional>
#include <span>

#include "./tuple_data.hpp"
#include "pgreplication/pgoutput/options.hpp"
#include "pgreplication/utils.hpp"

namespace PGREPLICATION_NAMESPACE::pgoutput::events {
template <BinaryValue Binary, StreamingEnabledValue Streaming>
struct Update;

template <BinaryValue Binary>
struct Update<Binary, StreamingEnabledValue::ON> {
    std::int32_t transactionId;
    std::int32_t oid;
    std::optional<OldDataOrPrimaryKeyTupleData<Binary>> oldDataOrPrimaryKey;
    TupleData<Binary> data;

    constexpr static std::size_t minBufferSize =
        sizeof(transactionId) + sizeof(oid) + sizeof(std::int16_t);
    using input_buffer = std::span<char>;

    constexpr static Update<Binary, StreamingEnabledValue::ON> fromBuffer(
        const input_buffer &buffer) {
        const auto &transactionId =
            ::PGREPLICATION_NAMESPACE::utils::int32FromNetwork(
                buffer.subspan<0, 4>());
        const auto &oid = ::PGREPLICATION_NAMESPACE::utils::int32FromNetwork(
            buffer.subspan<4, 4>());
        const auto &[oldDataOrPrimaryKey, readBytes] =
            parseOldDataOrPrimaryKey<Binary>(buffer.subspan<8>());
        return { .transactionId = transactionId,
                 .oid = oid,
                 .oldDataOrPrimaryKey = oldDataOrPrimaryKey,
                 .data = parseTupleData<Binary>(
                             buffer.subspan(8 + sizeof('N') + readBytes))
                             .first };
    };
};

template <BinaryValue Binary>
struct Update<Binary, StreamingEnabledValue::OFF> {
    std::int32_t oid;
    std::optional<OldDataOrPrimaryKeyTupleData<Binary>> oldDataOrPrimaryKey;
    TupleData<Binary> data;

    constexpr static std::size_t minBufferSize =
        sizeof(oid) + sizeof(std::int16_t);
    using input_buffer = std::span<char>;

    static Update<Binary, StreamingEnabledValue::OFF> fromBuffer(
        const input_buffer &buffer) {
        const auto &oid = ::PGREPLICATION_NAMESPACE::utils::int32FromNetwork(
            buffer.subspan<0, 4>());
        const auto &[oldDataOrPrimaryKey, readBytes] =
            parseOldDataOrPrimaryKey<Binary>(buffer.subspan<4>());
        return { .oid = oid,
                 .oldDataOrPrimaryKey = oldDataOrPrimaryKey,
                 .data = parseTupleData<Binary>(
                             buffer.subspan(4 + sizeof('N') + readBytes))
                             .first };
    };
};
};  // namespace PGREPLICATION_NAMESPACE::pgoutput::events

namespace std {
template <PGREPLICATION_NAMESPACE::pgoutput::BinaryValue Binary>
struct formatter<PGREPLICATION_NAMESPACE::pgoutput::events::Update<
    Binary, PGREPLICATION_NAMESPACE::pgoutput::StreamingEnabledValue::ON>> {
    template <typename ParseContext>
    constexpr auto parse(ParseContext &ctx) {
        return ctx.begin();
    }

    template <typename FormatContext>
    auto format(
        const PGREPLICATION_NAMESPACE::pgoutput::events::Update<
            Binary,
            PGREPLICATION_NAMESPACE::pgoutput::StreamingEnabledValue::ON>
            &record,
        FormatContext &ctx) const {
        return format_to(ctx.out(),
                         "Update(transactionId: {}, oid: {}, "
                         "oldDataOrPrimaryKey: {}, data: {})",
                         record.transactionId, record.oid,
                         record.oldDataOrPrimaryKey, record.data);
    }
};

template <PGREPLICATION_NAMESPACE::pgoutput::BinaryValue Binary>
struct formatter<PGREPLICATION_NAMESPACE::pgoutput::events::Update<
    Binary, PGREPLICATION_NAMESPACE::pgoutput::StreamingEnabledValue::OFF>> {
    template <typename ParseContext>
    constexpr auto parse(ParseContext &ctx) {
        return ctx.begin();
    }

    template <typename FormatContext>
    auto format(
        const PGREPLICATION_NAMESPACE::pgoutput::events::Update<
            Binary,
            PGREPLICATION_NAMESPACE::pgoutput::StreamingEnabledValue::OFF>
            &record,
        FormatContext &ctx) const {
        return format_to(ctx.out(),
                         "Update(oid: {}, oldDataOrPrimaryKey: {}, data: {})",
                         record.oid, record.oldDataOrPrimaryKey, record.data);
    }
};
};  // namespace std
