#pragma once

#include <cassert>
#include <cstddef>
#include <expected>
#include <optional>
#include <span>
#include <string>
#include <variant>

#include "./begin.hpp"
#include "./commit.hpp"
#include "./delete.hpp"
#include "./insert.hpp"
#include "./relation.hpp"
#include "./truncate.hpp"
#include "./type.hpp"
#include "./update.hpp"
#include "pgreplication/pgoutput/events/utils.hpp"
#include "pgreplication/pgoutput/options.hpp"

namespace PGREPLICATION_NAMESPACE::pgoutput::events {
enum class BaseEventType {
    BEGIN = 'B',
    COMMIT = 'C',
    RELATION = 'R',
    TYPE = 'Y',
    INSERT = 'I',
    UPDATE = 'U',
    DELETE = 'D',
    TRUNCATE = 'T'
};

std::optional<BaseEventType> parseBaseEvenType(const char &c);

template <BinaryValue Binary, StreamingEnabledValue StreamingEnabled>
using BaseEvent =
    std::variant<Begin, Commit, Relation<StreamingEnabled>,
                 Type<StreamingEnabled>, Insert<Binary, StreamingEnabled>,
                 Update<Binary, StreamingEnabled>,
                 Delete<Binary, StreamingEnabled>, Truncate<StreamingEnabled>>;

template <BinaryValue Binary, StreamingEnabledValue StreamingEnabled>
std::expected<BaseEvent<Binary, StreamingEnabled>, std::string> parseBaseEvent(
    const BaseEventType &eventType, const std::span<char> &buffer) {
    switch (eventType) {
        case BaseEventType::BEGIN:
            return utils::parseStaticSizeEvent<Begin>(buffer);
        case BaseEventType::COMMIT:
            return utils::parseStaticSizeEvent<Commit>(buffer);
        case BaseEventType::RELATION:
            return utils::parseDynamicSizeEvent<Relation<StreamingEnabled>>(
                buffer);
        case BaseEventType::TYPE:
            return utils::parseDynamicSizeEvent<Type<StreamingEnabled>>(buffer);
        case BaseEventType::INSERT:
            return utils::parseDynamicSizeEvent<
                Insert<Binary, StreamingEnabled>>(buffer);
        case BaseEventType::UPDATE:
            return utils::parseDynamicSizeEvent<
                Update<Binary, StreamingEnabled>>(buffer);
        case BaseEventType::DELETE:
            return utils::parseDynamicSizeEvent<
                Delete<Binary, StreamingEnabled>>(buffer);
        case BaseEventType::TRUNCATE:
            return utils::parseDynamicSizeEvent<Truncate<StreamingEnabled>>(
                buffer);
    };
};

};  // namespace PGREPLICATION_NAMESPACE::pgoutput::events
