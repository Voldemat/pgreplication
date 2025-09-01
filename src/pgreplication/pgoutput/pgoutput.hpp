#pragma once

#include <string>

#include "./events/event.hpp"
#include "./options.hpp"

namespace PGREPLICATION_NAMESPACE::pgoutput {
template <BinaryValue Binary, MessagesValue Messages, StreamingValue Streaming,
          TwoPhaseValue TwoPhase, OriginValue OriginInfo>
struct SessionContext {
    using Event =
        events::Event<Binary, Messages, Streaming, TwoPhase, OriginInfo>;
    constexpr static auto parseEvent =
        events::parseEvent<Binary, Messages, Streaming, TwoPhase, OriginInfo>;

    constexpr static std::string buildStaticOptions() {
        return buildPgoutputStaticOptions<Binary, Messages, Streaming, TwoPhase,
                                          OriginInfo>();
    };
};
};  // namespace PGREPLICATION_NAMESPACE::pgoutput
