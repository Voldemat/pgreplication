#include "./utils.hpp"

#include <arpa/inet.h>
#include <sys/_endian.h>

#include <cassert>
#include <cstdint>
#include <format>
#include <stdexcept>

namespace PGREPLICATION_NAMESPACE::utils {
std::int64_t int64FromNetwork(const type_span<std::int64_t> &buffer) {
    return ntohll(*(std::int64_t *)buffer.data());
};

std::int32_t int32FromNetwork(const type_span<std::int32_t> &buffer) {
    return ntohl(*(std::int32_t *)buffer.data());
};

std::int16_t int16FromNetwork(const type_span<std::int16_t> &buffer) {
    return ntohs(*(std::int16_t *)buffer.data());
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
    *(std::uint64_t *)buffer.data() = htonll(n);
};

void int32ToNetwork(const type_span<std::int32_t> &buffer, std::int32_t n) {
    *(std::uint32_t *)buffer.data() = htonl(n);
};

void boolToNetwork(const type_span<bool> &buffer, bool value) {
    *buffer.data() = value ? 1 : 0;
};
};  // namespace PGREPLICATION_NAMESPACE::utils
