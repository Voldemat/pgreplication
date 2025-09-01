#include "./begin.hpp"

#include "pgreplication/utils.hpp"

using namespace PGREPLICATION_NAMESPACE::utils;
namespace PGREPLICATION_NAMESPACE::pgoutput::events {

Begin Begin::fromBuffer(const input_buffer &buffer) {
    return {
        .finalTransactionLsn = int64FromNetwork(buffer.subspan<0, 8>()),
        .commitTimestamp = int64FromNetwork(buffer.subspan<8, 8>()),
        .transactionId = int32FromNetwork(buffer.subspan<16, 4>()),
    };
};

};  // namespace PGREPLICATION_NAMESPACE::pgoutput::events
