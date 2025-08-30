#include "./utils.hpp"

#include <arpa/inet.h>

#include <cassert>
#include <cstdint>
#include <format>
#include <span>
#include <stdexcept>

namespace PGREPLICATION_NAMESPACE::utils {
std::int64_t int64FromNetwork(const std::span<char> &buffer) {
    assert(buffer.size() == 8);
    return ntohll(*(std::int64_t *)&buffer[0]);
};

std::int32_t int32FromNetwork(const std::span<char> &buffer) {
    assert(buffer.size() == 4);
    return ntohl(*(std::int32_t *)&buffer[0]);
};

bool boolFromNetwork(const char c) {
    switch (c) {
        case '1':
            return true;
        case '0':
            return false;
    }
    throw std::runtime_error(std::format("Unknown value for bool: {}", c));
};

void int64ToNetwork(char *pointer, std::int64_t n) {
    *(std::uint64_t *)pointer = htonll(n);
};

void int32ToNetwork(char *pointer, std::int32_t n) {
    *(std::uint32_t *)pointer = htonl(n);
};

void boolToNetwork(char* pointer, bool value) {
    *pointer = value ? '1' : '0';
};
};  // namespace PGREPLICATION_NAMESPACE::utils
