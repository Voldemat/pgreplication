#pragma once

#include <arpa/inet.h>

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <format>
#include <span>
#include <tuple>
#include <type_traits>
#include <variant>
#include <vector>

namespace PGREPLICATION_NAMESPACE::utils {
template <typename T>
using type_span = std::span<char, sizeof(T)>;
std::int64_t int64FromNetwork(const type_span<std::int64_t> &buffer);
std::int32_t int32FromNetwork(const type_span<std::int32_t> &buffer);
std::int16_t int16FromNetwork(const type_span<std::int16_t> &buffer);
bool boolFromNetwork(const char c);
void int64ToNetwork(const type_span<std::int64_t> &buffer, std::int64_t n);
void int32ToNetwork(const type_span<std::int32_t> &buffer, std::int32_t n);
void boolToNetwork(const type_span<bool> &buffer, bool value);

template <class... Ts>
struct overloaded : Ts... {
    using Ts::operator()...;
};
template <class... Ts>
overloaded(Ts...) -> overloaded<Ts...>;

template <typename... Ts>
struct filter_void;

template <typename T, typename... Ts>
struct filter_void<T, Ts...> {
    using type = decltype(std::tuple_cat(
        std::declval<std::conditional_t<std::is_void_v<T>, std::tuple<>,
                                        std::tuple<T>>>(),
        std::declval<typename filter_void<Ts...>::type>()));
};

template <>
struct filter_void<> {
    using type = std::tuple<>;
};

template <typename... Ts>
using filter_void_t = typename filter_void<Ts...>::type;

// Convert tuple -> variant
template <typename Tuple>
struct tuple_to_variant;

template <typename... Ts>
struct tuple_to_variant<std::tuple<Ts...>> {
    using type = std::variant<Ts...>;
};

template <typename... Ts>
using make_variant_t = typename tuple_to_variant<filter_void_t<Ts...>>::type;

};  // namespace PGREPLICATION_NAMESPACE::utils

namespace std {
#ifdef PGREPLICATION_ADD_STD_VARIANT_FORMATTER
template <typename... Ts>
struct formatter<variant<Ts...>> {
    template <typename ParseContext>
    constexpr auto parse(ParseContext &ctx) {
        return ctx.begin();
    }

    template <typename FormatContext>
    auto format(const variant<Ts...> &record, FormatContext &ctx) const {
        return std::visit(
            [&ctx](auto &&arg) { return std::format_to(ctx.out(), "{}", arg); },
            record);
    }
};
#endif

template <>
struct formatter<vector<byte>> {
    template <typename ParseContext>
    constexpr auto parse(ParseContext &ctx) {
        return ctx.begin();
    }

    template <typename FormatContext>
    auto format(const std::vector<std::byte> &record,
                FormatContext &ctx) const {
        return std::format_to(ctx.out(), "{}",
                              std::span<unsigned char>(
                                  const_cast<unsigned char *>(
                                      reinterpret_cast<const unsigned char *>(
                                          record.begin().base())),
                                  record.size()));
    }
};

};  // namespace std
