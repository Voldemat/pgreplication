#pragma once

#include <cstddef>
#include <cstdint>
#include <expected>
#include <format>
#include <optional>
#include <span>
#include <string>
#include <variant>

namespace PGREPLICATION_NAMESPACE::pgoutput::events {
enum class TwoPhaseCommitEventType {
    BEGIN_PREPARE = 'b',
    PREPARE = 'P',
    COMMIT_PREPARED = 'K',
    ROLLBACK_PREPARED = 'r',
};

std::optional<TwoPhaseCommitEventType> parseTwoPhaseCommitEventType(
    const char &c);

struct BeginPrepare {
    std::int64_t lsn;
    std::int64_t endLsn;
    std::int64_t timestamp;
    std::int32_t transactionId;
    std::string gid;

    constexpr static std::size_t minBufferSize = sizeof(lsn) + sizeof(endLsn) +
                                                 sizeof(timestamp) +
                                                 sizeof(transactionId) + 1;
    using input_buffer = std::span<char>;

    static BeginPrepare fromBuffer(const input_buffer &buffer);
};

struct Prepare {
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

    static Prepare fromBuffer(const input_buffer &buffer);
};

struct CommitPrepared {
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

    static CommitPrepared fromBuffer(const input_buffer &buffer);
};

struct RollbackPrepared {
    std::int8_t flags;
    std::int64_t lsn;
    std::int64_t endLsn;
    std::int64_t prepareTimestamp;
    std::int64_t rollbackTimestamp;
    std::int32_t transactionId;
    std::string gid;

    constexpr static std::size_t minBufferSize =
        sizeof(flags) + sizeof(lsn) + sizeof(endLsn) +
        sizeof(prepareTimestamp) + sizeof(rollbackTimestamp) +
        sizeof(transactionId) + 1;
    using input_buffer = std::span<char>;

    static RollbackPrepared fromBuffer(const input_buffer &buffer);
};

std::expected<
    std::variant<BeginPrepare, Prepare, CommitPrepared, RollbackPrepared>,
    std::string>
parseTwoPhaseCommitEvent(const TwoPhaseCommitEventType &eventType,
                         const std::span<char> &buffer);
};  // namespace PGREPLICATION_NAMESPACE::pgoutput::events

namespace std {
template <>
struct std::formatter<PGREPLICATION_NAMESPACE::pgoutput::events::BeginPrepare> {
    template <typename ParseContext>
    constexpr auto parse(ParseContext &ctx) {
        return ctx.begin();
    }

    template <typename FormatContext>
    auto format(
        const PGREPLICATION_NAMESPACE::pgoutput::events::BeginPrepare &record,
        FormatContext &ctx) const {
        return std::format_to(ctx.out(),
                              "BeginPrepare(lsn: {}, endLsn: {}, timestamp: "
                              "{}, transactionId: {}, gid: {})",
                              record.lsn, record.endLsn, record.timestamp,
                              record.transactionId, record.gid);
    }
};

template <>
struct std::formatter<PGREPLICATION_NAMESPACE::pgoutput::events::Prepare> {
    template <typename ParseContext>
    constexpr auto parse(ParseContext &ctx) {
        return ctx.begin();
    }

    template <typename FormatContext>
    auto format(
        const PGREPLICATION_NAMESPACE::pgoutput::events::Prepare &record,
        FormatContext &ctx) const {
        return std::format_to(
            ctx.out(),
            "Prepare(flags: {}, lsn: {}, endLsn: {}, timestamp: "
            "{}, transactionId: {}, gid: {})",
            record.flags, record.lsn, record.endLsn, record.timestamp,
            record.transactionId, record.gid);
    }
};

template <>
struct std::formatter<
    PGREPLICATION_NAMESPACE::pgoutput::events::CommitPrepared> {
    template <typename ParseContext>
    constexpr auto parse(ParseContext &ctx) {
        return ctx.begin();
    }

    template <typename FormatContext>
    auto format(
        const PGREPLICATION_NAMESPACE::pgoutput::events::CommitPrepared &record,
        FormatContext &ctx) const {
        return std::format_to(
            ctx.out(),
            "CommitPrepared(flags: {}, lsn: {}, endLsn: {}, timestamp: "
            "{}, transactionId: {}, gid: {})",
            record.flags, record.lsn, record.endLsn, record.timestamp,
            record.transactionId, record.gid);
    }
};

template <>
struct std::formatter<
    PGREPLICATION_NAMESPACE::pgoutput::events::RollbackPrepared> {
    template <typename ParseContext>
    constexpr auto parse(ParseContext &ctx) {
        return ctx.begin();
    }

    template <typename FormatContext>
    auto format(
        const PGREPLICATION_NAMESPACE::pgoutput::events::RollbackPrepared
            &record,
        FormatContext &ctx) const {
        return std::format_to(ctx.out(),
                              "RollbackPrepared(flags: {}, lsn: {}, endLsn: "
                              "{}, prepareTimestamp: {}, rollbackTimestamp: "
                              "{}, transactionId: {}, gid: {})",
                              record.flags, record.lsn, record.endLsn,
                              record.prepareTimestamp, record.rollbackTimestamp,
                              record.transactionId, record.gid);
    }
};
};  // namespace std
