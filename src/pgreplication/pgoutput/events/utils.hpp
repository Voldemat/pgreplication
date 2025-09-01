#pragma once

#include <concepts>
#include <cstddef>
#include <expected>
#include <format>
#include <functional>
#include <span>
#include <string>

namespace PGREPLICATION_NAMESPACE::pgoutput::events::utils {
template <typename T>
concept StaticSizeEvent = requires(T a) {
    { T::bufferSize } -> std::convertible_to<std::size_t>;
    {
        T::fromBuffer
    }
    -> std::convertible_to<std::function<T(const typename T::input_buffer &)>>;
};

template <StaticSizeEvent T>
std::expected<T, std::string> parseStaticSizeEvent(
    const std::span<char> &buffer) {
    if (buffer.size() != T::bufferSize) {
        return std::unexpected(std::format("{} event buffer size must be {}",
                                           typeid(T).name(), T::bufferSize));
    };
    return T::fromBuffer(buffer.subspan<0, T::bufferSize>());
};

template <typename T>
concept DynamicSizeEvent = requires(T a) {
    { T::minBufferSize } -> std::convertible_to<std::size_t>;
    {
        T::fromBuffer
    }
    -> std::convertible_to<std::function<T(const typename T::input_buffer &)>>;
};

template <DynamicSizeEvent T>
std::expected<T, std::string> parseDynamicSizeEvent(
    const std::span<char> &buffer) {
    if (buffer.size() < T::minBufferSize) {
        return std::unexpected(
            std::format("{} event buffer size must be gte {}", typeid(T).name(),
                        T::minBufferSize));
    };
    return T::fromBuffer(buffer.subspan<0>());
};

};  // namespace PGREPLICATION_NAMESPACE::pgoutput::events::utils
