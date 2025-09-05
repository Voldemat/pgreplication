#pragma once

#include <cstddef>
#include <cstdint>
#include <format>
#include <span>
#include <string>

namespace PGREPLICATION_NAMESPACE::pgoutput::events {

enum class StreamingAndTwoPhaseCommitEventType { STREAM_PREPARE = 'p' };

struct StreamPrepare {
    std::int8_t flags;
    std::int64_t lsn;
    std::int64_t endLsn;
    std::int64_t timestamp;
    std::int32_t transactionId;
    std::string gid;

    constexpr static std::size_t minBufferSize =
        sizeof(flags) + sizeof(lsn) + sizeof(endLsn) + sizeof(timestamp) +
        sizeof(transactionId) + 1;
    using input_buffer = std::span<char>;

    static StreamPrepare fromBuffer(const input_buffer &buffer);
};
};  // namespace PGREPLICATION_NAMESPACE::pgoutput::events

namespace std {
template <>
struct formatter<PGREPLICATION_NAMESPACE::pgoutput::events::StreamPrepare> {
    template <typename ParseContext>
    constexpr auto parse(ParseContext &ctx) {
        return ctx.begin();
    }

    template <typename FormatContext>
    auto format(
        const PGREPLICATION_NAMESPACE::pgoutput::events::StreamPrepare &record,
        FormatContext &ctx) const {
        return format_to(ctx.out(),
                         "StreamPrepare(flags: {}, lsn: {}, endLsn: {}, "
                         "timestamp: {}, transactionId: {}, gid: {})",
                         record.flags, record.lsn, record.endLsn,
                         record.timestamp, record.transactionId, record.gid);
    }
};
};  // namespace std
