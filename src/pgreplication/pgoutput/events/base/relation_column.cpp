#include "./relation_column.hpp"

#include <cstdint>
#include <span>
#include <string>
#include <vector>

#include "pgreplication/utils.hpp"

using namespace PGREPLICATION_NAMESPACE::utils;
namespace PGREPLICATION_NAMESPACE::pgoutput::events {

RelationColumn RelationColumn::fromBuffer(const input_buffer &buffer) {
    const auto &name = std::string(buffer.subspan(1).data());
    return {
        .flags = static_cast<std::int8_t>(buffer.subspan<0, 1>().front()),
        .name = name,
        .oid = int32FromNetwork(
            buffer.subspan(1 + 1 + name.size(), 4).subspan<0, 4>()),
        .typeModifier = int32FromNetwork(
            buffer.subspan(1 + 1 + name.size() + 4, 4).subspan<0, 4>()),
    };
};

std::vector<RelationColumn> parseRelationColumns(
    const std::int16_t &columnCount, const std::span<char> &buffer) {
    std::vector<RelationColumn> columns;
    unsigned int bufferPosition = 0;
    for (std::int16_t index = 0; index < columnCount; index++) {
        const auto &column =
            RelationColumn::fromBuffer(buffer.subspan(bufferPosition));
        columns.emplace_back(column);
        bufferPosition += RelationColumn::minBufferSize + column.name.size();
    };
    return columns;
};
};  // namespace PGREPLICATION_NAMESPACE::pgoutput::events
