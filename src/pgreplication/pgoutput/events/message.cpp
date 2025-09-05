#include "./message.hpp"

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "../options.hpp"
#include "pgreplication/utils.hpp"

namespace PGREPLICATION_NAMESPACE::pgoutput::events {
Message<StreamingEnabledValue::ON>
Message<StreamingEnabledValue::ON>::fromBuffer(const input_buffer &buffer) {
    const auto &transactionId = utils::int32FromNetwork(buffer.subspan<0, 4>());
    const auto &flags =
        static_cast<std::int8_t>(buffer.subspan<4, 1>().front());
    const auto &lsn = utils::int64FromNetwork(buffer.subspan<5, 8>());
    const auto &prefix = std::string(buffer.subspan<13>().data());
    const auto &contentLength = utils::int32FromNetwork(
        buffer.subspan(13 + 1 + prefix.size(), 4).subspan<0, 4>());
    const auto &contentBuffer =
        buffer.subspan(13 + 1 + prefix.size() + 4, contentLength);
    std::vector<std::byte> content(
        reinterpret_cast<std::byte *>(contentBuffer.begin().base()),
        reinterpret_cast<std::byte *>(contentBuffer.end().base()));
    return {
        .transactionId = transactionId,
        .flags = flags,
        .lsn = lsn,
        .prefix = prefix,
        .content = content,
    };
};

Message<StreamingEnabledValue::OFF>
Message<StreamingEnabledValue::OFF>::fromBuffer(const input_buffer &buffer) {
    const auto &flags =
        static_cast<std::int8_t>(buffer.subspan<0, 1>().front());
    const auto &lsn = utils::int64FromNetwork(buffer.subspan<1, 8>());
    const auto &prefix = std::string(buffer.subspan<9>().data());
    const auto &contentLength = utils::int32FromNetwork(
        buffer.subspan(9 + 1 + prefix.size(), 4).subspan<0, 4>());
    const auto &contentBuffer =
        buffer.subspan(9 + 1 + prefix.size() + 4, contentLength);
    std::vector<std::byte> content(
        reinterpret_cast<std::byte *>(contentBuffer.begin().base()),
        reinterpret_cast<std::byte *>(contentBuffer.end().base()));
    return {
        .flags = flags,
        .lsn = lsn,
        .prefix = prefix,
        .content = content,
    };
};

};  // namespace PGREPLICATION_NAMESPACE::pgoutput::events
