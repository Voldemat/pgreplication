#include "./relation.hpp"

#include <span>
#include <string>

#include "./relation_column.hpp"
#include "pgreplication/pgoutput/options.hpp"
#include "pgreplication/utils.hpp"

using namespace PGREPLICATION_NAMESPACE::utils;
namespace PGREPLICATION_NAMESPACE::pgoutput::events {

Relation<StreamingEnabledValue::ON>
Relation<StreamingEnabledValue::ON>::fromBuffer(const input_buffer &buffer) {
    const auto &transactionId = int32FromNetwork(buffer.subspan<0, 4>());
    const auto &oid = int32FromNetwork(buffer.subspan<4, 4>());
    const auto &relationNamespace = std::string(buffer.subspan(8).data());
    const auto &afterNamespaceIndex = 8 + 1 + relationNamespace.size();
    const auto &name = std::string(buffer.subspan(afterNamespaceIndex).data());
    const auto &afterNameIndex = afterNamespaceIndex + 1 + name.size();
    const auto &replicaIdentity = buffer.subspan(afterNameIndex, 1).front();
    const auto &columnCount =
        int16FromNetwork(buffer.subspan(afterNameIndex + 1, 2).subspan<0, 2>());
    return { .transactionId = transactionId,
             .oid = oid,
             .relationNamespace = relationNamespace,
             .name = name,
             .replicaIdentity = replicaIdentity,
             .columns = parseRelationColumns(
                 columnCount, buffer.subspan(afterNameIndex + 2)) };
};

Relation<StreamingEnabledValue::OFF>
Relation<StreamingEnabledValue::OFF>::fromBuffer(const input_buffer &buffer) {
    const auto &oid = int32FromNetwork(buffer.subspan<0, 4>());
    const auto &relationNamespace = std::string(buffer.subspan(4).data());
    const auto &afterNamespaceIndex = 4 + 1 + relationNamespace.size();
    const auto &name = std::string(buffer.subspan(afterNamespaceIndex).data());
    const auto &afterNameIndex = afterNamespaceIndex + 1 + name.size();
    const auto &replicaIdentity = buffer.subspan(afterNameIndex, 1).front();
    const auto &columnCount =
        int16FromNetwork(buffer.subspan(afterNameIndex + 1, 2).subspan<0, 2>());
    return { .oid = oid,
             .relationNamespace = relationNamespace,
             .name = name,
             .replicaIdentity = replicaIdentity,
             .columns = parseRelationColumns(
                 columnCount, buffer.subspan(afterNameIndex + 2)) };
};
};  // namespace PGREPLICATION_NAMESPACE::pgoutput::events
