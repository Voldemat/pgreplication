#include "./pgoutput.hpp"

#include <string>

#include "pgreplication/pgoutput/options.hpp"

using namespace pgreplication::pgoutput;
void check2(const std::string &something) {

};

using Context = SessionContext<BinaryValue::OFF, MessagesValue::ON, StreamingValue::OFF,
                       TwoPhaseValue::OFF, OriginValue::NONE>;

void check() {
    check2(Context::buildStaticOptions());
};
