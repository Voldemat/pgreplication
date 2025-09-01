#pragma once

#include <format>
namespace PGREPLICATION_NAMESPACE::pgoutput {
enum class StreamingValue { ON, OFF, PARALLEL };
enum class StreamingEnabledValue { ON, OFF };
enum class BinaryValue { ON, OFF };
enum class MessagesValue { ON, OFF };
enum class TwoPhaseValue { ON, OFF };
enum class OriginValue { ON, OFF };
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
};  // namespace PGREPLICATION_NAMESPACE::pgoutput
