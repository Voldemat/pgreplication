#include "./stream.hpp"

#include <optional>

#include "pgreplication/pgoutput/options.hpp"
#include "pgreplication/utils.hpp"

using namespace PGREPLICATION_NAMESPACE::utils;
namespace PGREPLICATION_NAMESPACE::pgoutput::events {
std::optional<StreamingEventType> parseStreamingEventType(const char &c) {
    const auto &casted = static_cast<StreamingEventType>(c);
    switch (casted) {
        case StreamingEventType::STREAM_START:
        case StreamingEventType::STREAM_STOP:
        case StreamingEventType::STREAM_COMMIT:
        case StreamingEventType::STREAM_ABORT:
            return casted;
        default:
            return std::nullopt;
    };
};

StreamStart StreamStart::fromBuffer(const input_buffer &buffer) {
    return {
        .transactionId = int32FromNetwork(buffer.subspan<0, 4>()),
        .flags = buffer.subspan<4, 1>().front(),
    };
};

StreamCommit StreamCommit::fromBuffer(const input_buffer &buffer) {
    return { .transactionId = int32FromNetwork(buffer.subspan<0, 4>()),
             .flags = buffer.subspan<4, 1>().front(),
             .lsn = int64FromNetwork(buffer.subspan<5, 8>()),
             .endLsn = int64FromNetwork(buffer.subspan<13, 8>()),
             .timestamp = int64FromNetwork(buffer.subspan<21, 8>()) };
};

StreamAbort<StreamingValue::PARALLEL>
StreamAbort<StreamingValue::PARALLEL>::fromBuffer(const input_buffer &buffer) {
    return { .transactionId = int32FromNetwork(buffer.subspan<0, 4>()),
             .subTransactionId =
                 int32FromNetwork(buffer.subspan<4, 4>()),
             .lsn = int32FromNetwork(buffer.subspan<8, 4>()),
             .timestamp = int64FromNetwork(buffer.subspan<12, 8>()) };
};

StreamAbort<StreamingValue::ON> StreamAbort<StreamingValue::ON>::fromBuffer(
    const input_buffer &buffer) {
    return { .transactionId = int32FromNetwork(buffer.subspan<0, 4>()),
             .subTransactionId =
                 int32FromNetwork(buffer.subspan<4, 4>()) };
};

};  // namespace PGREPLICATION_NAMESPACE::pgoutput::events
