#include "../events.hpp"

#include <arpa/inet.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <cstring>
#include <span>

#include "../utils.hpp"

using namespace PGREPLICATION_NAMESPACE;

TEST(PrimaryKeepaliveMessage, TestFromNetworkBufferConstructor) {
    char buffer[PrimaryKeepaliveMessage::size];
    std::span<char, PrimaryKeepaliveMessage::size> bufferSpan = buffer;
    utils::int64ToNetwork(bufferSpan.subspan<0, 8>(), 1);
    utils::int64ToNetwork(bufferSpan.subspan<8, 8>(), 2);
    utils::boolToNetwork(bufferSpan.subspan<16, 1>(), false);
    const auto &result = PrimaryKeepaliveMessage::fromNetworkBuffer(bufferSpan);
    EXPECT_EQ(result.serverWalEnd, 1);
    EXPECT_EQ(result.sentAtUnixTimestamp, 2);
    EXPECT_EQ(result.replyRequested, false);
}

TEST(StandbyStatusUpdate, TestFromNetworkBufferConstructor) {
    char buffer[StandbyStatusUpdate::size];
    std::span<char, StandbyStatusUpdate::size> bufferSpan = buffer;
    utils::int64ToNetwork(bufferSpan.subspan<0, 8>(), 1);
    utils::int64ToNetwork(bufferSpan.subspan<8, 8>(), 2);
    utils::int64ToNetwork(bufferSpan.subspan<16, 8>(), 3);
    utils::int64ToNetwork(bufferSpan.subspan<24, 8>(), 4);
    utils::boolToNetwork(bufferSpan.subspan<32, 1>(), true);
    const auto &result =
        pgreplication::StandbyStatusUpdate::fromNetworkBuffer(bufferSpan);
    EXPECT_EQ(result.writtenWalPosition, 1);
    EXPECT_EQ(result.flushedWalPosition, 2);
    EXPECT_EQ(result.appliedWalPosition, 3);
    EXPECT_EQ(result.sentAtUnixTimestamp, 4);
    EXPECT_EQ(result.replyRequested, true);
}

TEST(HotStandbyFeedbackMessage, TestFromNetworkBufferConstructor) {
    char buffer[pgreplication::HotStandbyFeedbackMessage::size];
    std::span<char, HotStandbyFeedbackMessage::size> bufferSpan = buffer;
    utils::int64ToNetwork(bufferSpan.subspan<0, 8>(), 1);
    utils::int32ToNetwork(bufferSpan.subspan<8, 4>(), 2);
    utils::int32ToNetwork(bufferSpan.subspan<12, 4>(), 3);
    utils::int32ToNetwork(bufferSpan.subspan<16, 4>(), 4);
    utils::int32ToNetwork(bufferSpan.subspan<20, 4>(), 5);
    const auto &result =
        pgreplication::HotStandbyFeedbackMessage::fromNetworkBuffer(bufferSpan);
    EXPECT_EQ(result.sentAtUnixTimestamp, 1);
    EXPECT_EQ(result.xmin, 2);
    EXPECT_EQ(result.xminEpoch, 3);
    EXPECT_EQ(result.lowestReplicationSlotCatalogXmin, 4);
    EXPECT_EQ(result.catalogXminEpoch, 5);
}

TEST(XLogData, TestFromNetworkBufferConstructor) {
    char walData[] = "hello!";
    char buffer[pgreplication::XLogData::minSize + sizeof(walData)];
    std::span<char> bufferSpan(buffer, sizeof(buffer));
    utils::int64ToNetwork(bufferSpan.subspan<0, 8>(), 1);
    utils::int64ToNetwork(bufferSpan.subspan<8, 8>(), 2);
    utils::int64ToNetwork(bufferSpan.subspan<16, 8>(), 3);
    std::memcpy(bufferSpan.subspan(24, sizeof(walData)).data(), walData,
                sizeof(walData));
    const auto &result = pgreplication::XLogData::fromNetworkBuffer(bufferSpan);
    ASSERT_TRUE(result.has_value()) << result.error();
    const auto &data = result.value();
    EXPECT_EQ(data.messageWalStart, 1);
    EXPECT_EQ(data.serverWalEnd, 2);
    EXPECT_EQ(data.sentAtUnixTimestamp, 3);
    EXPECT_EQ(data.walData.size(), sizeof(walData));
    EXPECT_STREQ((const char *)data.walData.data(), (const char *)&walData[0]);
}
