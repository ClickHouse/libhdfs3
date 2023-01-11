/*
 * This file is subject to this repository: https://github.com/CPythoner/ByteBuffer.
 * We made some changes and optimizations.
 */
#include "gtest/gtest.h"
#include "gmock/gmock.h"

#include "client/ByteBuffer.h"

using namespace testing;

class TestByteBuffer: public ::testing::Test {
public:
    TestByteBuffer() {
    }
    ~TestByteBuffer() {
    }
};

TEST_F(TestByteBuffer, TestConstructor) {
    ByteBuffer bb;
    EXPECT_TRUE(bb.capacity() == DEFAULT_BUFFER_SIZE);
    EXPECT_TRUE(bb.limit()    == DEFAULT_BUFFER_SIZE);
    EXPECT_TRUE(bb.position() == 0);

    ByteBuffer bb2(10);
    EXPECT_TRUE(bb2.capacity() == 10);
    EXPECT_TRUE(bb2.limit()    == 10);
    EXPECT_TRUE(bb2.position() == 0);

    std::string str = "hello bytebuffer";
    ByteBuffer bb3((int8_t*)str.c_str(), static_cast<uint32_t>(str.size()));
    EXPECT_TRUE(bb3.capacity() == static_cast<uint32_t>(str.size()));
    EXPECT_TRUE(bb3.limit()    == static_cast<uint32_t>(str.size()));
    EXPECT_TRUE(bb3.position() == 0);
    int8_t ret[32] = {0};
    bb3.getBytes(ret, static_cast<uint32_t>(str.size()));
    EXPECT_TRUE(strcmp((const char*)ret, str.c_str()) == 0);
}

TEST_F(TestByteBuffer, TestCheckSize) {
    ByteBuffer bb(1);
    bb.put('5');
    EXPECT_TRUE(bb.capacity() == 1);
    bb.put(1);
    EXPECT_TRUE(bb.capacity() == 2049);
}

TEST_F(TestByteBuffer, TestPutAndGet) {
    std::string str = "hello bytebuffer";
    ByteBuffer bb;
    bb.put('5');
    bb.putShort(0x4567);
    bb.putInt(0xABCDEF98);
    bb.putLong(0x1234567890ABCDEF);
    bb.putFloat(2111.23f);
    bb.putDouble(35.5236245);
    bb.putBytes((int8_t*)str.c_str(), str.size());
    bb.printInfo();

    bb.flip();
    bb.printInfo();

    EXPECT_TRUE(bb.get() == '5');
    EXPECT_TRUE(bb.getShort() == 0x4567);
    EXPECT_TRUE(bb.getInt() == 0xABCDEF98);
    EXPECT_TRUE(bb.getLong() == 0x1234567890ABCDEF);

    const double EPS = 1e-6;
    EXPECT_TRUE(fabs(bb.getFloat() - 2111.23f) <= EPS);
    EXPECT_TRUE(fabs(bb.getDouble() - 35.5236245) <= EPS);

    int8_t retbytes[32] = {0};
    bb.getBytes(retbytes, str.size());
    EXPECT_TRUE(strcmp((char*)retbytes, str.c_str()) == 0);
}

TEST_F(TestByteBuffer, TestFlip) {
    ByteBuffer bb(1);
    bb.putBytes((int8_t*)"hello", 5);
    bb.flip();
    EXPECT_TRUE(bb.capacity() == 2049);
    EXPECT_TRUE(bb.limit() == 5);
    EXPECT_TRUE(bb.position() == 0);
}

TEST_F(TestByteBuffer, TestCompact) {
    ByteBuffer bb;
    bb.putBytes((int8_t*)"hello", 5);
    bb.flip();
    bb.get();
    bb.get(); // get two bytes, now position == 3
    bb.compact();
    EXPECT_TRUE(bb.limit() == bb.capacity());
    EXPECT_TRUE(bb.position() == 3);
}

TEST_F(TestByteBuffer, TestDuplicate) {
    ByteBuffer bb;
    bb.putChar('a');
    ByteBuffer *bb1 = bb.duplicate();
    EXPECT_TRUE(bb.equals(bb1));
    bb1->putChar('b');
    // The two buffers' position, limit values will be independent.
    EXPECT_TRUE(bb1->position() == 2);
    EXPECT_TRUE(bb1->limit() == 2048);
    EXPECT_TRUE(bb.position() == 1);
    EXPECT_TRUE(bb.limit() == 2048);
    // Changes to new buffer's content will be visible in the old buffer.
    EXPECT_TRUE(bb.getChar(1) == bb1->getChar(1));
}

TEST_F(TestByteBuffer, TestSlice) {
    ByteBuffer bb;
    bb.putBytes((int8_t*)"hello", 5);
    EXPECT_TRUE(bb.position() == 5);
    EXPECT_TRUE(bb.limit() == 2048);
    bb.position(2);
    bb.limit(3);
    EXPECT_TRUE(bb.position() == 2);
    EXPECT_TRUE(bb.limit() == 3);

    // The two buffers' position, limit values will be independent.
    ByteBuffer *bb1 = bb.slice();
    EXPECT_TRUE(bb1->position() == 0);
    EXPECT_TRUE(bb1->limit() == 1);
    EXPECT_TRUE(bb1->getChar(0) == 'l');

    // Changes to new buffer's content will be visible in the old buffer.
    bb1->put((int8_t)'w', static_cast<uint32_t>(0));
    EXPECT_TRUE(bb1->get(0) == 'w');
    EXPECT_TRUE(bb.get(2) == 'w');
}