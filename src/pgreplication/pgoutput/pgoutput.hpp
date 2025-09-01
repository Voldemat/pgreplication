#pragma once

#include <string_view>

#include "./events/event.hpp"
#include "./options.hpp"

namespace PGREPLICATION_NAMESPACE::pgoutput {
template <BinaryValue Binary, MessagesValue Messages, StreamingValue Streaming,
          TwoPhaseValue TwoPhase, OriginValue OriginInfo>
struct SessionContext {
    constexpr static std::string_view staticOptions =
        buildPgoutputStaticOptions<Binary, Messages, Streaming, TwoPhase,
                                   OriginInfo>();
    using Event = events::Event<Binary, Messages, Streaming, TwoPhase, OriginInfo>;
    constexpr static auto parseEvent =
        events::parseEvent<Binary, Messages, Streaming, TwoPhase, OriginInfo>;
};
};  // namespace PGREPLICATION_NAMESPACE::pgoutput
