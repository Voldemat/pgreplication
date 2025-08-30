#pragma once

#include <arpa/inet.h>

#include <cassert>
#include <cstdint>
#include <span>

namespace PGREPLICATION_NAMESPACE::utils {
std::int64_t int64FromNetwork(const std::span<char> &buffer);
std::int32_t int32FromNetwork(const std::span<char> &buffer);
bool boolFromNetwork(const char c);
void int64ToNetwork(char *pointer, std::int64_t n);
void int32ToNetwork(char *pointer, std::int32_t n);
void boolToNetwork(char *pointer, bool value);
};  // namespace PGREPLICATION_NAMESPACE::utils
