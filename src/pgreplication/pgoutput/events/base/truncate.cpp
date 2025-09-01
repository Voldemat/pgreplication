#include "./truncate.hpp"

#include <cstdint>
#include <span>
#include <vector>

#include "pgreplication/pgoutput/options.hpp"
#include "pgreplication/utils.hpp"

using namespace PGREPLICATION_NAMESPACE::utils;
namespace PGREPLICATION_NAMESPACE::pgoutput::events {
Truncate<StreamingEnabledValue::ON>
Truncate<StreamingEnabledValue::ON>::fromBuffer(const std::span<char> &buffer) {
    const auto &transactionId = int32FromNetwork(buffer.subspan<0, 4>());
    const auto &relationsCount = int32FromNetwork(buffer.subspan<4, 4>());
    const auto &flags = buffer.subspan<8, 1>().front();
    const auto &oidsBuffer =
        buffer.subspan(9, relationsCount * sizeof(std::int32_t));
    std::vector<std::int32_t> oids(oidsBuffer.begin(), oidsBuffer.end());
    return { .transactionId = transactionId, .flags = flags, .oids = oids };
};

Truncate<StreamingEnabledValue::OFF> Truncate<
    StreamingEnabledValue::OFF>::fromBuffer(const std::span<char> &buffer) {
    const auto &relationsCount = int32FromNetwork(buffer.subspan<0, 4>());
    const auto &flags = buffer.subspan<4, 1>().front();
    const auto &oidsBuffer =
        buffer.subspan(5, relationsCount * sizeof(std::int32_t));
    std::vector<std::int32_t> oids(oidsBuffer.begin(), oidsBuffer.end());
    return { .flags = flags, .oids = oids };
};
};  // namespace PGREPLICATION_NAMESPACE::pgoutput::events
