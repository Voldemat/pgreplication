#pragma once

#include <cstddef>
#include <cstdint>
#include <format>
#include <span>
#include <string>
#include <vector>

#include "./relation_column.hpp"
#include "pgreplication/pgoutput/options.hpp"

namespace PGREPLICATION_NAMESPACE::pgoutput::events {
template <StreamingEnabledValue Streaming>
struct Relation;

template <>
struct Relation<StreamingEnabledValue::ON> {
    std::int32_t transactionId;
    std::int32_t oid;
    std::string relationNamespace;
    std::string name;
    std::int8_t replicaIdentity;
    std::vector<RelationColumn> columns;

    constexpr static std::size_t minBufferSize =
        sizeof(transactionId) + sizeof(oid) + 1 + 1 + sizeof(replicaIdentity) +
        sizeof(std::int16_t);
    using input_buffer = std::span<char>;

    static Relation<StreamingEnabledValue::ON> fromBuffer(
        const input_buffer &buffer);
};

template <>
struct Relation<StreamingEnabledValue::OFF> {
    std::int32_t oid;
    std::string relationNamespace;
    std::string name;
    std::int8_t replicaIdentity;
    std::vector<RelationColumn> columns;
    constexpr static std::size_t minBufferSize =
        sizeof(oid) + 1 + 1 + sizeof(replicaIdentity) + sizeof(std::int16_t);
    using input_buffer = std::span<char>;

    static Relation<StreamingEnabledValue::OFF> fromBuffer(
        const input_buffer &buffer);
};

};  // namespace PGREPLICATION_NAMESPACE::pgoutput::events

namespace std {
template <>
struct std::formatter<PGREPLICATION_NAMESPACE::pgoutput::events::Relation<
    PGREPLICATION_NAMESPACE::pgoutput::StreamingEnabledValue::ON>> {
    template <typename ParseContext>
    constexpr auto parse(ParseContext &ctx) {
        return ctx.begin();
    }

    template <typename FormatContext>
    auto format(
        const PGREPLICATION_NAMESPACE::pgoutput::events::Relation<
            PGREPLICATION_NAMESPACE::pgoutput::StreamingEnabledValue::ON>
            &record,
        FormatContext &ctx) const {
        return std::format_to(ctx.out(),
                              "Relation(transactionId: {}, oid: {}, namespace: "
                              "{}, name: {}, replicaIdentity: {}, columns: {})",
                              record.transactionId, record.oid,
                              record.relationNamespace, record.name,
                              record.replicaIdentity, record.columns);
    }
};

template <>
struct formatter<PGREPLICATION_NAMESPACE::pgoutput::events::Relation<
    PGREPLICATION_NAMESPACE::pgoutput::StreamingEnabledValue::OFF>> {
    template <typename ParseContext>
    constexpr auto parse(ParseContext &ctx) {
        return ctx.begin();
    }

    template <typename FormatContext>
    auto format(
        const PGREPLICATION_NAMESPACE::pgoutput::events::Relation<
            PGREPLICATION_NAMESPACE::pgoutput::StreamingEnabledValue::OFF>
            &record,
        FormatContext &ctx) const {
        return format_to(ctx.out(),
                         "Relation(oid: {}, namespace: {}, name: {}, "
                         "replicaIdentity: {}, columns: {})",
                         record.oid, record.relationNamespace, record.name,
                         record.replicaIdentity, record.columns);
    }
};
};  // namespace std
