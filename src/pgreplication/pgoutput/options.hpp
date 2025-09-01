#pragma once

#include <string>

namespace PGREPLICATION_NAMESPACE::pgoutput {
enum class StreamingValue { ON, OFF, PARALLEL };
enum class StreamingEnabledValue { ON, OFF };
enum class BinaryValue { ON, OFF };
enum class MessagesValue { ON, OFF };
enum class TwoPhaseValue { ON, OFF };
enum class OriginValue { NONE, ANY };
enum class IsParallelValue { TRUE, FALSE };

constexpr StreamingEnabledValue streamingValueToStreamingEnabledValue(
    const StreamingValue &value) {
    switch (value) {
        case StreamingValue::ON:
            return StreamingEnabledValue::ON;
        case StreamingValue::PARALLEL:
            return StreamingEnabledValue::ON;
        case StreamingValue::OFF:
            return StreamingEnabledValue::OFF;
    };
};

constexpr std::string binaryValueToString(const BinaryValue &value) {
    return value == BinaryValue::ON ? "true" : "false";
};

constexpr std::string messagesValueToString(const MessagesValue &value) {
    return value == MessagesValue::ON ? "true" : "false";
};

constexpr std::string streamingValueToString(const StreamingValue &value) {
    switch (value) {
        case StreamingValue::ON:
            return "on";
        case StreamingValue::OFF:
            return "off";
        case StreamingValue::PARALLEL:
            return "parallel";
    };
};

constexpr std::string twoPhaseValueToString(const TwoPhaseValue &value) {
    return value == TwoPhaseValue::ON ? "true" : "false";
};

constexpr std::string originValueToString(const OriginValue &value) {
    switch (value) {
        case OriginValue::ANY:
            return "any";
        case OriginValue::NONE:
            return "none";
    };
};

template <BinaryValue Binary, MessagesValue Messages, StreamingValue Streaming,
          TwoPhaseValue TwoPhase, OriginValue OriginInfo>
constexpr std::string buildPgoutputStaticOptions() {
    return "proto_version '4', binary '" + binaryValueToString(Binary) +
           "', messages '" + messagesValueToString(Messages) +
           "', streaming '" + streamingValueToString(Streaming) +
           "', two_phase '" + twoPhaseValueToString(TwoPhase) + "', origin '" +
           originValueToString(OriginInfo) + "'";
};
};  // namespace PGREPLICATION_NAMESPACE::pgoutput
