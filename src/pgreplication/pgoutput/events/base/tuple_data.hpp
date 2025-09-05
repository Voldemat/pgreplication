#pragma once

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <format>
#include <optional>
#include <span>
#include <string>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#include "pgreplication/pgoutput/options.hpp"
#include "pgreplication/utils.hpp"

namespace PGREPLICATION_NAMESPACE::pgoutput::events {
struct PGNull {};
struct PGUnchangedToastedValue {};

template <BinaryValue Binary>
using TupleDataColumn =
    std::variant<PGNull, PGUnchangedToastedValue,
                 std::conditional_t<Binary == BinaryValue::ON,
                                    std::vector<std::byte>, std::string>>;

template <BinaryValue Binary>
using TupleData = std::vector<TupleDataColumn<Binary>>;

template <BinaryValue Binary>
constexpr std::pair<TupleDataColumn<Binary>, unsigned int> parseTupleColumn(
    const std::span<char> &buffer) {
    const auto &c = buffer.front();
    switch (c) {
        case 'n':
            return { PGNull{}, 1 };
        case 'u':
            return { PGUnchangedToastedValue{}, 1 };
    };
    const auto &valueSize = ::PGREPLICATION_NAMESPACE::utils::int32FromNetwork(
        buffer.subspan<1, 4>());
    const auto &valueBuffer = buffer.subspan(5, valueSize);
    if constexpr (Binary == BinaryValue::ON) {
        assert(c == 'b');
        return { std::vector<std::byte>(
                     reinterpret_cast<std::byte *>(valueBuffer.begin().base()),
                     reinterpret_cast<std::byte *>(valueBuffer.end().base())),
                 5 + valueSize };
    } else {
        assert(c == 't');
        return { std::string(valueBuffer.begin(), valueBuffer.end()),
                 5 + valueSize };
    };
};

template <BinaryValue Binary>
constexpr std::pair<TupleData<Binary>, unsigned int> parseTupleData(
    const std::span<char> &buffer) {
    TupleData<Binary> data;
    const auto &columnSize = ::PGREPLICATION_NAMESPACE::utils::int16FromNetwork(
        buffer.subspan<0, 2>());
    unsigned int bufferPosition = 2;
    for (std::int16_t index = 0; index < columnSize; index++) {
        const auto &[column, readBytes] =
            parseTupleColumn<Binary>(buffer.subspan(bufferPosition));
        data.emplace_back(column);
        bufferPosition += readBytes;
    };
    return { data, bufferPosition };
};

template <BinaryValue Binary>
using OldTupleData = TupleData<Binary>;
template <BinaryValue Binary>
using PrimaryKeyTupleData = TupleData<Binary>;

template <BinaryValue Binary>
using OldDataOrPrimaryKeyTupleData =
    std::variant<OldTupleData<Binary>, PrimaryKeyTupleData<Binary>>;

template <BinaryValue Binary>
std::pair<std::optional<OldDataOrPrimaryKeyTupleData<Binary>>, unsigned int>
parseOldDataOrPrimaryKey(const std::span<char> &buffer) {
    if (buffer.size() == 0) return { std::nullopt, 0 };
    const auto &c = buffer.front();
    switch (c) {
        case 'K': {
            const auto &[tupleData, readBytes] =
                parseTupleData<Binary>(buffer.subspan<1>());
            return { OldDataOrPrimaryKeyTupleData<Binary>(
                         std::in_place_index<1>, tupleData),
                     readBytes + 1 };
        }
        case 'O':
            const auto &[tupleData, readBytes] =
                parseTupleData<Binary>(buffer.subspan<1>());
            return { OldDataOrPrimaryKeyTupleData<Binary>(
                         std::in_place_index<0>, tupleData),
                     readBytes + 1 };
    };
    return { std::nullopt, 0 };
};

};  // namespace PGREPLICATION_NAMESPACE::pgoutput::events

namespace std {
template <>
struct formatter<PGREPLICATION_NAMESPACE::pgoutput::events::PGNull> {
    template <typename ParseContext>
    constexpr auto parse(ParseContext &ctx) {
        return ctx.begin();
    }

    template <typename FormatContext>
    auto format(const PGREPLICATION_NAMESPACE::pgoutput::events::PGNull &record,
                FormatContext &ctx) const {
        return format_to(ctx.out(), "NULL");
    }
};

template <>
struct formatter<
    PGREPLICATION_NAMESPACE::pgoutput::events::PGUnchangedToastedValue> {
    template <typename ParseContext>
    constexpr auto parse(ParseContext &ctx) {
        return ctx.begin();
    }

    template <typename FormatContext>
    auto format(
        const PGREPLICATION_NAMESPACE::pgoutput::events::PGUnchangedToastedValue
            &record,
        FormatContext &ctx) const {
        return format_to(ctx.out(), "PGUnchangedToastedValue");
    }
};
};  // namespace std
