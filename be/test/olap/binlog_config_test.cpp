#include "olap/binlog_config.h"

#include <gtest/gtest.h>

#include "olap/binlog_config.h"

class BinlogConfigTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Set up any test data or variables here
    }

    void TearDown() override {
        // Clean up any test data or variables here
    }

    // Declare any helper functions here
};

TEST_F(BinlogConfigTest, AssignmentOperatorT) {
    TBinlogConfig config;
    config.enable = true;
    config.ttl_seconds = 3600;
    config.max_bytes = 1024;
    config.max_history_nums = 10;

    BinlogConfig binlog_config;
    binlog_config = config;

    EXPECT_EQ(binlog_config.enable(), true);
    EXPECT_EQ(binlog_config.ttl_seconds(), 3600);
    EXPECT_EQ(binlog_config.max_bytes(), 1024);
    EXPECT_EQ(binlog_config.max_history_nums(), 10);
}

TEST_F(BinlogConfigTest, AssignmentOperatorPB) {
    BinlogConfigPB config_pb;
    config_pb.set_enable(true);
    config_pb.set_ttl_seconds(3600);
    config_pb.set_max_bytes(1024);
    config_pb.set_max_history_nums(10);

    BinlogConfig binlog_config;
    binlog_config = config_pb;

    EXPECT_EQ(binlog_config.enable(), true);
    EXPECT_EQ(binlog_config.ttl_seconds(), 3600);
    EXPECT_EQ(binlog_config.max_bytes(), 1024);
    EXPECT_EQ(binlog_config.max_history_nums(), 10);
}

TEST_F(BinlogConfigTest, ToPB) {
    BinlogConfig binlog_config;
    binlog_config.set_enable(true);
    binlog_config.set_ttl_seconds(3600);
    binlog_config.set_max_bytes(1024);
    binlog_config.set_max_history_nums(10);

    BinlogConfigPB config_pb;
    binlog_config.to_pb(&config_pb);

    EXPECT_EQ(config_pb.enable(), true);
    EXPECT_EQ(config_pb.ttl_seconds(), 3600);
    EXPECT_EQ(config_pb.max_bytes(), 1024);
    EXPECT_EQ(config_pb.max_history_nums(), 10);
}

TEST_F(BinlogConfigTest, ToString) {
    BinlogConfig binlog_config;
    binlog_config.set_enable(true);
    binlog_config.set_ttl_seconds(3600);
    binlog_config.set_max_bytes(1024);
    binlog_config.set_max_history_nums(10);

    std::string expected =
            "BinlogConfig enable: true, ttl_seconds: 3600, max_bytes: 1024, max_history_nums: 10";
    EXPECT_EQ(binlog_config.to_string(), expected);
}

// Test default constructor
TEST(BinlogConfigTest, DefaultConstructorTest) {
    BinlogConfig config;
    EXPECT_FALSE(config.is_enable());
    EXPECT_EQ(config.ttl_seconds(), std::numeric_limits<int64_t>::max());
    EXPECT_EQ(config.max_bytes(), std::numeric_limits<int64_t>::max());
    EXPECT_EQ(config.max_history_nums(), std::numeric_limits<int64_t>::max());
}

// Test setters and getters
TEST(BinlogConfigTest, SettersAndGettersTest) {
    BinlogConfig config;
    config.set_enable(true);
    EXPECT_TRUE(config.is_enable());
    config.set_ttl_seconds(100);
    EXPECT_EQ(config.ttl_seconds(), 100);
    config.set_max_bytes(200);
    EXPECT_EQ(config.max_bytes(), 200);
    config.set_max_history_nums(300);
    EXPECT_EQ(config.max_history_nums(), 300);
}

// Test copy constructor
TEST(BinlogConfigTest, CopyConstructorTest) {
    BinlogConfig config;
    config.set_enable(true);
    config.set_ttl_seconds(100);
    config.set_max_bytes(200);
    config.set_max_history_nums(300);

    BinlogConfig config2(config);
    EXPECT_TRUE(config2.is_enable());
    EXPECT_EQ(config2.ttl_seconds(), 100);
    EXPECT_EQ(config2.max_bytes(), 200);
    EXPECT_EQ(config2.max_history_nums(), 300);
}

// Test assignment operator
TEST(BinlogConfigTest, AssignmentOperatorTest) {
    BinlogConfig config;
    config.set_enable(true);
    config.set_ttl_seconds(100);
    config.set_max_bytes(200);
    config.set_max_history_nums(300);

    BinlogConfig config3;
    config3 = config;
    EXPECT_TRUE(config3.is_enable());
    EXPECT_EQ(config3.ttl_seconds(), 100);
    EXPECT_EQ(config3.max_bytes(), 200);
    EXPECT_EQ(config3.max_history_nums(), 300);
}

// Test to_pb() and to_string()
TEST(BinlogConfigTest, ToPbAndToStringTest) {
    BinlogConfig config;
    config.set_enable(true);
    config.set_ttl_seconds(100);
    config.set_max_bytes(200);
    config.set_max_history_nums(300);

    BinlogConfigPB pb;
    config.to_pb(&pb);
    EXPECT_EQ(pb.enable(), true);
    EXPECT_EQ(pb.ttl_seconds(), 100);
    EXPECT_EQ(pb.max_bytes(), 200);
    EXPECT_EQ(pb.max_history_nums(), 300);

    std::string str = config.to_string();
    EXPECT_EQ(str, "enable: true, ttl_seconds: 100, max_bytes: 200, max_history_nums: 300");
}
