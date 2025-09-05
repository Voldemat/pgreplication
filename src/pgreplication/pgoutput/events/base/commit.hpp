#pragma once

#include <cstddef>
#include <cstdint>
#include <format>
#include <span>

namespace PGREPLICATION_NAMESPACE::pgoutput::events {
struct Commit {
    std::int8_t flags;
    std::int64_t lsn;
    std::int64_t endLsn;
    std::int64_t timestamp;

    constexpr static std::size_t bufferSize =
        sizeof(flags) + sizeof(lsn) + sizeof(endLsn) + sizeof(timestamp);
    using input_buffer = std::span<char, bufferSize>;

    static Commit fromBuffer(const input_buffer &buffer);
};

};  // namespace PGREPLICATION_NAMESPACE::pgoutput::events

namespace std {
template <>
struct formatter<PGREPLICATION_NAMESPACE::pgoutput::events::Commit> {
    template <typename ParseContext>
    constexpr auto parse(ParseContext &ctx) {
        return ctx.begin();
    }

    template <typename FormatContext>
    auto format(const PGREPLICATION_NAMESPACE::pgoutput::events::Commit &record,
                FormatContext &ctx) const {
        return format_to(
            ctx.out(), "Commit(flags: {}, lsn: {}, endLsn: {}, timestamp: {})",
            record.flags, record.lsn, record.endLsn, record.timestamp);
    }
};
};  // namespace std
