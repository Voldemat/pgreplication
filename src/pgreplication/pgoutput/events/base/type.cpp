#include "./type.hpp"

#include <span>
#include <string>

#include "pgreplication/pgoutput/options.hpp"
#include "pgreplication/utils.hpp"

using namespace PGREPLICATION_NAMESPACE::utils;
namespace PGREPLICATION_NAMESPACE::pgoutput::events {
Type<StreamingEnabledValue::ON> Type<StreamingEnabledValue::ON>::fromBuffer(
    const input_buffer &buffer) {
    const auto &transactionId = int32FromNetwork(buffer.subspan<0, 4>());
    const auto &oid = int32FromNetwork(buffer.subspan<4, 4>());
    const auto &typeNamespace = std::string(buffer.subspan<8>().data());
    const auto &name =
        std::string(buffer.subspan(8 + 1 + typeNamespace.size()).data());
    return {
        .transactionId = transactionId,
        .oid = oid,
        .typeNamespace = typeNamespace,
        .name = name,
    };
};

Type<StreamingEnabledValue::OFF> Type<StreamingEnabledValue::OFF>::fromBuffer(
    const input_buffer &buffer) {
    const auto &oid = int32FromNetwork(buffer.subspan<0, 4>());
    const auto &typeNamespace = std::string(buffer.subspan<4>().data());
    const auto &name =
        std::string(buffer.subspan(4 + 1 + typeNamespace.size()).data());
    return {
        .oid = oid,
        .typeNamespace = typeNamespace,
        .name = name,
    };
};

};  // namespace PGREPLICATION_NAMESPACE::pgoutput::events
