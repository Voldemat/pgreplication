#include "./utils.hpp"

#include <bit>
#include <cassert>
#include <cstdint>
#include <format>
#include <stdexcept>

namespace PGREPLICATION_NAMESPACE::utils {
std::int64_t int64FromNetwork(const type_span<std::int64_t> &buffer) {
    const auto &value = *(std::int64_t *)buffer.data();
    if constexpr (std::endian::native == std::endian::little) {
        return value;
    };
    return std::byteswap(value);
};

std::int32_t int32FromNetwork(const type_span<std::int32_t> &buffer) {
    const auto &value = *(std::int32_t *)buffer.data();
    if constexpr (std::endian::native == std::endian::little) {
        return value;
    };
    return std::byteswap(value);
};

std::int16_t int16FromNetwork(const type_span<std::int16_t> &buffer) {
    const auto &value = *(std::int16_t *)buffer.data();
    if constexpr (std::endian::native == std::endian::little) {
        return value;
    };
    return std::byteswap(value);
};

bool boolFromNetwork(const char c) {
    switch (c) {
        case 1:
            return true;
        case 0:
            return false;
    }
    throw std::runtime_error(
        std::format("Unknown value for bool: charpoint - {}, char - {}",
                    (std::int8_t)c, c));
};

void int64ToNetwork(const type_span<std::int64_t> &buffer, std::int64_t n) {
    if constexpr (std::endian::native == std::endian::little) {
        *(std::uint64_t *)buffer.data() = n;
    } else {
        *(std::uint64_t *)buffer.data() = std::byteswap(n);
    };
};

void int32ToNetwork(const type_span<std::int32_t> &buffer, std::int32_t n) {
    if constexpr (std::endian::native == std::endian::little) {
        *(std::uint32_t *)buffer.data() = n;
    } else {
        *(std::uint32_t *)buffer.data() = std::byteswap(n);
    };
};

void boolToNetwork(const type_span<bool> &buffer, bool value) {
    *buffer.data() = value ? 1 : 0;
};
};  // namespace PGREPLICATION_NAMESPACE::utils
