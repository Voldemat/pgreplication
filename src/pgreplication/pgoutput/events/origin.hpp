#pragma once

#include <cstddef>
#include <cstdint>
#include <format>
#include <span>
#include <string>

namespace PGREPLICATION_NAMESPACE::pgoutput::events {
enum class OriginEventType { ORIGIN = 'O' };
struct Origin {
    std::int64_t commitLsn;
    std::string origin;

    constexpr static std::size_t minBufferSize = sizeof(commitLsn) + 1;
    using input_buffer = std::span<char>;

    static Origin fromBuffer(const input_buffer &buffer);
};
};  // namespace PGREPLICATION_NAMESPACE::pgoutput::events

namespace std {
template <>
struct std::formatter<PGREPLICATION_NAMESPACE::pgoutput::events::Origin> {
    template <typename ParseContext>
    constexpr auto parse(ParseContext &ctx) {
        return ctx.begin();
    }

    template <typename FormatContext>
    auto format(const PGREPLICATION_NAMESPACE::pgoutput::events::Origin &record,
                FormatContext &ctx) const {
        return std::format_to(ctx.out(), "Origin(commitLsn: {}, origin: {})",
                              record.commitLsn, record.origin);
    }
};
};  // namespace std
