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
    utils::int64ToNetwork(buffer, 1);
    utils::int64ToNetwork(&buffer[8], 2);
    utils::boolToNetwork(&buffer[16], false);
    const auto &bufferSpan = std::span<char, PrimaryKeepaliveMessage::size>(
        buffer, PrimaryKeepaliveMessage::size);
    const auto &result = PrimaryKeepaliveMessage::fromNetworkBuffer(bufferSpan);
    EXPECT_EQ(result.serverWalEnd, 1);
    EXPECT_EQ(result.sentAtUnixTimestamp, 2);
    EXPECT_EQ(result.replyRequested, false);
}

TEST(StandbyStatusUpdate, TestFromNetworkBufferConstructor) {
    char buffer[StandbyStatusUpdate::size];
    utils::int64ToNetwork(buffer, 1);
    utils::int64ToNetwork(&buffer[8], 2);
    utils::int64ToNetwork(&buffer[16], 3);
    utils::int64ToNetwork(&buffer[24], 4);
    buffer[32] = 1;
    const auto &bufferSpan =
        std::span<char, pgreplication::StandbyStatusUpdate::size>(
            buffer, pgreplication::StandbyStatusUpdate::size);
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
    utils::int64ToNetwork(buffer, 1);
    utils::int32ToNetwork(&buffer[8], 2);
    utils::int32ToNetwork(&buffer[12], 3);
    utils::int32ToNetwork(&buffer[16], 4);
    utils::int32ToNetwork(&buffer[20], 5);
    const auto &bufferSpan =
        std::span<char, pgreplication::HotStandbyFeedbackMessage::size>(
            buffer, pgreplication::HotStandbyFeedbackMessage::size);
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
    utils::int64ToNetwork(buffer, 1);
    utils::int64ToNetwork(&buffer[8], 2);
    utils::int64ToNetwork(&buffer[16], 3);
    std::memcpy(&buffer[24], walData, sizeof(walData));
    const auto &bufferSpan = std::span<char>(buffer, sizeof(buffer));
    const auto &result = pgreplication::XLogData::fromNetworkBuffer(bufferSpan);
    ASSERT_TRUE(result.has_value()) << result.error();
    const auto &data = result.value();
    EXPECT_EQ(data.messageWalStart, 1);
    EXPECT_EQ(data.serverWalEnd, 2);
    EXPECT_EQ(data.sentAtUnixTimestamp, 3);
    EXPECT_EQ(data.walData.size(), sizeof(walData));
    EXPECT_STREQ((const char *)data.walData.data(), (const char *)&walData[0]);
}
