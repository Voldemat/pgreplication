#pragma once

#include <cstddef>
#include <cstdint>
#include <format>
#include <span>
#include <string>
#include <vector>

namespace PGREPLICATION_NAMESPACE::pgoutput::events {
struct RelationColumn {
    std::int8_t flags;
    std::string name;
    std::int32_t oid;
    std::int32_t typeModifier;

    constexpr static std::size_t minBufferSize =
        sizeof(flags) + 1 + sizeof(oid) + sizeof(typeModifier);
    using input_buffer = std::span<char>;

    static RelationColumn fromBuffer(const input_buffer &buffer);
};

std::vector<RelationColumn> parseRelationColumns(
    const std::int16_t &columnCount, const std::span<char> &buffer);

};  // namespace PGREPLICATION_NAMESPACE::pgoutput::events

namespace std {
template <>
struct formatter<PGREPLICATION_NAMESPACE::pgoutput::events::RelationColumn> {
    template <typename ParseContext>
    constexpr auto parse(ParseContext &ctx) {
        return ctx.begin();
    }

    template <typename FormatContext>
    auto format(
        const PGREPLICATION_NAMESPACE::pgoutput::events::RelationColumn &record,
        FormatContext &ctx) const {
        return format_to(
            ctx.out(),
            "RelationColumn(flags: {}, name: {}, oid: {}, typeModifier: {})",
            record.flags, record.name, record.oid, record.typeModifier);
    }
};
};  // namespace std
