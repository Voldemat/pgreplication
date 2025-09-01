#include "./commit.hpp"

#include "pgreplication/utils.hpp"

using namespace PGREPLICATION_NAMESPACE::utils;
namespace PGREPLICATION_NAMESPACE::pgoutput::events {
Commit Commit::fromBuffer(const input_buffer &buffer) {
    return {
        .flags = buffer.subspan<0, 1>().front(),
        .lsn = int64FromNetwork(buffer.subspan<1, 8>()),
        .endLsn = int64FromNetwork(buffer.subspan<9, 8>()),
        .timestamp = int64FromNetwork(buffer.subspan<17, 8>()),
    };
};

};  // namespace PGREPLICATION_NAMESPACE::pgoutput::events
