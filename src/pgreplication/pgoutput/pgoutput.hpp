#pragma once

#include <string>
#include <type_traits>

#include "./events/event.hpp"
#include "./options.hpp"
#include "pgreplication/pgoutput/events/base/begin.hpp"
#include "pgreplication/pgoutput/events/base/commit.hpp"
#include "pgreplication/pgoutput/events/base/delete.hpp"
#include "pgreplication/pgoutput/events/base/insert.hpp"
#include "pgreplication/pgoutput/events/base/relation.hpp"
#include "pgreplication/pgoutput/events/base/truncate.hpp"
#include "pgreplication/pgoutput/events/base/type.hpp"
#include "pgreplication/pgoutput/events/base/update.hpp"
#include "pgreplication/pgoutput/events/message.hpp"
#include "pgreplication/pgoutput/events/origin.hpp"
#include "pgreplication/pgoutput/events/stream.hpp"
#include "pgreplication/pgoutput/events/stream_and_twophase.hpp"
#include "pgreplication/pgoutput/events/twophase.hpp"

namespace PGREPLICATION_NAMESPACE::pgoutput {
template <BinaryValue TBinary, MessagesValue TMessages,
          StreamingValue TStreaming, TwoPhaseValue TTwoPhase,
          OriginValue TOriginInfo>
struct SessionContext {
    constexpr static auto Binary = TBinary;
    constexpr static auto Messages = TMessages;
    constexpr static auto Streaming = TStreaming;
    constexpr static auto StreamingEnabled =
        streamingValueToStreamingEnabledValue(Streaming);
    constexpr static auto TwoPhase = TTwoPhase;
    constexpr static auto OriginInfo = TOriginInfo;
    using Event =
        events::Event<Binary, Messages, Streaming, TwoPhase, OriginInfo>;

    constexpr static auto parseEvent =
        events::parseEvent<Binary, Messages, Streaming, TwoPhase, OriginInfo>;

    constexpr static std::string buildStaticOptions() {
        return buildPgoutputStaticOptions<Binary, Messages, Streaming, TwoPhase,
                                          OriginInfo>();
    };

    struct events {
        using Begin = PGREPLICATION_NAMESPACE::pgoutput::events::Begin;
        using Message = PGREPLICATION_NAMESPACE::pgoutput::events::Message<
            StreamingEnabled>;
        using Commit = PGREPLICATION_NAMESPACE::pgoutput::events::Commit;
        using Origin = PGREPLICATION_NAMESPACE::pgoutput::events::Origin;
        using Relation = PGREPLICATION_NAMESPACE::pgoutput::events::Relation<
            StreamingEnabled>;
        using Type =
            PGREPLICATION_NAMESPACE::pgoutput::events::Type<StreamingEnabled>;
        using Insert =
            PGREPLICATION_NAMESPACE::pgoutput::events::Insert<Binary,
                                                              StreamingEnabled>;
        using Update =
            PGREPLICATION_NAMESPACE::pgoutput::events::Update<Binary,
                                                              StreamingEnabled>;
        using Delete =
            PGREPLICATION_NAMESPACE::pgoutput::events::Delete<Binary,
                                                              StreamingEnabled>;
        using Truncate = PGREPLICATION_NAMESPACE::pgoutput::events::Truncate<
            StreamingEnabled>;

        using StreamStart =
            PGREPLICATION_NAMESPACE::pgoutput::events::StreamStart;
        using StreamStop =
            PGREPLICATION_NAMESPACE::pgoutput::events::StreamStop;
        using StreamCommit =
            PGREPLICATION_NAMESPACE::pgoutput::events::StreamCommit;
        using StreamAbort = std::conditional_t<
            StreamingEnabled == StreamingEnabledValue::ON,
            PGREPLICATION_NAMESPACE::pgoutput::events::StreamAbort<Streaming>,
            void>;

        using BeginPrepare =
            PGREPLICATION_NAMESPACE::pgoutput::events::BeginPrepare;
        using Prepare = PGREPLICATION_NAMESPACE::pgoutput::events::Prepare;
        using CommitPrepared =
            PGREPLICATION_NAMESPACE::pgoutput::events::CommitPrepared;
        using RollbackPrepared =
            PGREPLICATION_NAMESPACE::pgoutput::events::RollbackPrepared;
        using StreamPrepare =
            PGREPLICATION_NAMESPACE::pgoutput::events::StreamPrepare;
    };
};
};  // namespace PGREPLICATION_NAMESPACE::pgoutput
