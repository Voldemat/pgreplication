#include "./origin.hpp"
#include <string>

#include "pgreplication/utils.hpp"

namespace PGREPLICATION_NAMESPACE::pgoutput::events {
Origin Origin::fromBuffer(const input_buffer &buffer) {
    const auto &origin = buffer.subspan<8>();
    return { .commitLsn = utils::int64FromNetwork(buffer.subspan<0, 8>()),
             .origin = std::string(origin.data(), origin.size()) };
};
};  // namespace PGREPLICATION_NAMESPACE::pgoutput::events
