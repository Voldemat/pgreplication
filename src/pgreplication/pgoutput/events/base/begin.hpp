#pragma once

#include <cstddef>
#include <cstdint>
#include <format>
#include <span>

namespace PGREPLICATION_NAMESPACE::pgoutput::events {
struct Begin {
    std::int64_t finalTransactionLsn;
    std::int64_t commitTimestamp;
    std::int32_t transactionId;

    constexpr static std::size_t bufferSize = sizeof(finalTransactionLsn) +
                                              sizeof(commitTimestamp) +
                                              sizeof(transactionId);
    using input_buffer = std::span<char, bufferSize>;

    Begin static fromBuffer(const input_buffer &buffer);
};

};  // namespace PGREPLICATION_NAMESPACE::pgoutput::events

namespace std {
template <>
struct std::formatter<PGREPLICATION_NAMESPACE::pgoutput::events::Begin> {
    template <typename ParseContext>
    constexpr auto parse(ParseContext &ctx) {
        return ctx.begin();
    }

    template <typename FormatContext>
    auto format(const PGREPLICATION_NAMESPACE::pgoutput::events::Begin &record,
                FormatContext &ctx) const {
        return std::format_to(ctx.out(),
                              "Begin(finalTransactionLsn: {}, commitTimestamp: "
                              "{}, transactionId: {})",
                              record.finalTransactionLsn,
                              record.commitTimestamp, record.transactionId);
    }
};
};  // namespace std
