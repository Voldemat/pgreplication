#include "./event.hpp"

#include <optional>

namespace PGREPLICATION_NAMESPACE::pgoutput::events {
std::optional<BaseEventType> parseBaseEvenType(const char &c) {
    const auto &casted = static_cast<BaseEventType>(c);
    switch (casted) {
        case BaseEventType::BEGIN:
        case BaseEventType::COMMIT:
        case BaseEventType::RELATION:
        case BaseEventType::TYPE:
        case BaseEventType::INSERT:
        case BaseEventType::UPDATE:
        case BaseEventType::DELETE:
        case BaseEventType::TRUNCATE:
            return casted;
        default:
            return std::nullopt;
    };
};
};  // namespace PGREPLICATION_NAMESPACE::pgoutput::events
