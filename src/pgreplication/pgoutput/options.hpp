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

namespace std {
template <>
struct std::formatter<PGREPLICATION_NAMESPACE::pgoutput::BinaryValue> {
    template <typename ParseContext>
    constexpr auto parse(ParseContext &ctx) {
        return ctx.begin();
    }

    template <typename FormatContext>
    auto format(const PGREPLICATION_NAMESPACE::pgoutput::BinaryValue &record,
                FormatContext &ctx) const {
        return std::format_to(
            ctx.out(), "{}",
            record == PGREPLICATION_NAMESPACE::pgoutput::BinaryValue::ON
                ? "ON"
                : "OFF");
    }
};
};  // namespace std
