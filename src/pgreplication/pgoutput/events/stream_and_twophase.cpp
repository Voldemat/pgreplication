#include "./stream_and_twophase.hpp"

#include <string>

#include "pgreplication/utils.hpp"

namespace PGREPLICATION_NAMESPACE::pgoutput::events {
StreamPrepare StreamPrepare::fromBuffer(const input_buffer &buffer) {
    return { .flags = buffer.subspan<0, 1>().front(),
             .lsn = utils::int64FromNetwork(buffer.subspan<1, 8>()),
             .endLsn = utils::int64FromNetwork(buffer.subspan<9, 8>()),
             .timestamp = utils::int64FromNetwork(buffer.subspan<17, 8>()),
             .transactionId = utils::int32FromNetwork(buffer.subspan<25, 4>()),
             .gid = std::string(buffer.subspan<29>().data()) };
};
};  // namespace PGREPLICATION_NAMESPACE::pgoutput::events
