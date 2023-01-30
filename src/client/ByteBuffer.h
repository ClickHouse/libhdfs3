/*
 * This file is subject to this repository: https://github.com/CPythoner/ByteBuffer.
 * We made some changes and optimizations.
 */
#ifndef _HDFS_LIBHDFS3_CLIENT_BYTEBUFFER_H_
#define _HDFS_LIBHDFS3_CLIENT_BYTEBUFFER_H_

#include <cstdlib>
#include <string>
#include <iostream>

#include "Exception.h"
#include "ExceptionInternal.h"

// Default size of the buffer
#define DEFAULT_BUFFER_SIZE 2048

class ByteBuffer {
public:
    ByteBuffer(uint32_t capacity = DEFAULT_BUFFER_SIZE, const char * name = "", bool alloc = true)
            : name_(name),
              mark_(-1),
              limit_(capacity),
              position_(0),
              capacity_(capacity),
              alloc_buffer_(alloc) {
        p_buffer_ = nullptr;
        if (alloc_buffer_) {
            p_buffer_ = (int8_t *) calloc(capacity_, sizeof(int8_t));
        }
    }

    ByteBuffer(int8_t * arr, uint32_t length, const char * name = "")
            : name_(name), 
              mark_(-1),
              limit_(length),
              position_(0),
              capacity_(length)
               {
        p_buffer_ = nullptr;
        p_buffer_ = (int8_t *) calloc(capacity_, sizeof(int8_t));

        putBytes(arr, capacity_);
        clear();
    }

    ~ByteBuffer() {
        if (alloc_buffer_ && p_buffer_) {
            free(p_buffer_);
        }
        p_buffer_ = nullptr;
    }

    // Write Methods
    ByteBuffer & put(ByteBuffer * bb) {
        for (uint32_t i = 0; i < bb->limit(); i++)
            append<int8_t>(bb->get(i));

        return *this;
    }

    ByteBuffer & put(int8_t value) {
        append<int8_t>(value);
        return *this;
    }

    ByteBuffer & put(int8_t value, uint32_t index) {
        insert<int8_t>(value, index);
        return *this;
    }

    inline void putInt8_t(int8_t value, uint32_t index) {
        p_buffer_[index] = value;
    }

    ByteBuffer & putBytes(const int8_t * buf, uint32_t len) {
        for (uint32_t i = 0; i < len; i++)
            append<int8_t>(buf[i]);

        return *this;
    }

    inline void putInt8_ts(const int8_t * buf, uint32_t len) {
        if (!p_buffer_)
            return;
        checkSize(len);
        memcpy(&p_buffer_[position_], buf, len);
        position_ += len;
    }

    ByteBuffer & putBytes(const int8_t * buf, uint32_t len, uint32_t index) {
        position_ = index;
        for (uint32_t i = 0; i < len; i++)
            append<int8_t>(buf[i]);

        return *this;
    }

    ByteBuffer & putChar(char value) {
        append<int8_t>(value);
        return *this;
    }

    ByteBuffer & putChar(char value, uint32_t index) {
        insert<int8_t>(value, index);
        return *this;
    }

    ByteBuffer & putShort(uint16_t value) {
        append<uint16_t>(value);
        return *this;
    }

    ByteBuffer & putShort(uint16_t value, uint32_t index) {
        insert<uint16_t>(value, index);
        return *this;
    }

    ByteBuffer & putInt(uint32_t value) {
        append<uint32_t>(value);
        return *this;
    }

    ByteBuffer & putInt(uint32_t value, uint32_t index) {
        insert<uint32_t>(value, index);
        return *this;
    }

    ByteBuffer & putLong(uint64_t value) {
        append<uint64_t>(value);
        return *this;
    }

    ByteBuffer & putLong(uint64_t value, uint32_t index) {
        insert<uint64_t>(value, index);
        return *this;
    }

    ByteBuffer & putFloat(float value) {
        append<float>(value);
        return *this;
    }

    ByteBuffer & putFloat(float value, uint32_t index) {
        insert<float>(value, index);
        return *this;
    }

    ByteBuffer & putDouble(double value) {
        append<double>(value);
        return *this;
    }

    ByteBuffer & putDouble(double value, uint32_t index) {
        insert<double>(value, index);
        return *this;
    }

    // Read Methods
    int8_t get() {
        return read<int8_t>();
    }

    int8_t get(uint32_t index) const {
        return read<int8_t>(index);
    }

    inline int8_t getInt8_t(uint32_t index) const {
        return p_buffer_[index];
    }

    void getBytes(int8_t * buf, uint32_t len) {
        if (!p_buffer_ || position_ + len > limit_)
            return;

        for (uint32_t i = 0; i < len; i++) {
            buf[i] = p_buffer_[position_++];
        }
    }

    void getBytes(uint32_t index, int8_t * buf, uint32_t len) const {
        if (!p_buffer_ || index + len > limit_)
            return;

        uint32_t pos = index;
        for (uint32_t i = 0; i < len; i++) {
            buf[i] = p_buffer_[pos++];
        }
    }

    char getChar() {
        return read<int8_t>();
    }

    char getChar(uint32_t index) const {
        return read<int8_t>(index);
    }

    uint16_t getShort() {
        return read<uint16_t>();
    }

    uint16_t getShort(uint32_t index) const {
        return read<uint16_t>(index);
    }

    uint32_t getInt() {
        return read<uint32_t>();
    }

    uint32_t getInt(uint32_t index) const {
        return read<uint32_t>(index);
    }

    uint64_t getLong() {
        return read<uint64_t>();
    }

    uint64_t getLong(uint32_t index) const {
        return read<uint64_t>(index);
    }

    float getFloat() {
        return read<float>();
    }

    float getFloat(uint32_t index) const {
        return read<float>(index);
    }

    double getDouble() {
        return read<double>();
    }

    double getDouble(uint32_t index) const {
        return read<double>(index);
    }

    bool equals(ByteBuffer * other) {
        uint32_t len = limit();
        if (len != other->limit())
            return false;

        for (uint32_t i = 0; i < len; i++) {
            if (get(i) != other->get(i))
                return false;
        }

        return true;
    }

    ByteBuffer * duplicate() {
        ByteBuffer * newBuffer = new ByteBuffer(capacity_, "", false);

        newBuffer->p_buffer_ = this->p_buffer_;

        newBuffer->limit(limit_);
        newBuffer->position(position_);

        return newBuffer;
    }

    ByteBuffer * slice() {
        ByteBuffer *newBuffer = new ByteBuffer(remaining(), "", false);

        newBuffer->p_buffer_ = this->p_buffer_ + position_;

        newBuffer->limit(remaining());
        newBuffer->position(0);

        return newBuffer;
    }

    ByteBuffer & clear() {
        position_ = 0;
        mark_ = -1;
        limit_ = capacity_;
        return *this;
    }

    ByteBuffer & flip() {
        limit_ = position_;
        position_ = 0;
        mark_ = -1;
        return *this;
    }

    ByteBuffer & mark() {
        mark_ = position_;
        return *this;
    }

    ByteBuffer & discardMark() {
        mark_ = -1;
        return *this;
    }

    ByteBuffer & reset() {
        if (mark_ >= 0)
            position_ = mark_;

        return *this;
    }

    ByteBuffer & rewind() {
        mark_ = -1;
        position_ = 0;

        return *this;
    }

    ByteBuffer & compact() {
        do {
            if (position_ >= limit_) {
                position_ = 0;
                break;
            }

            for (uint32_t i = 0; i < limit_ - position_; i++) {
                p_buffer_[i] = p_buffer_[position_ + i];
            }
            position_ = limit_ - position_;
        } while (0);

        limit_ = capacity_;
        return *this;
    }

    bool hasRemaining() {
        return limit_ > position_;
    }

    uint32_t remaining() const {
        return position_ < limit_ ? limit_ - position_ : 0;
    }

    uint32_t capacity() const {
        return capacity_;
    }

    uint32_t position() const {
        return position_;
    }

    uint32_t limit() const {
        return limit_;
    }

    ByteBuffer & limit(uint32_t newLimit) {
        if (position_ > newLimit)
            position_ = newLimit;

        if (mark_ > (int32_t)newLimit)
            mark_ = -1;

        limit_ = newLimit;

        return *this;
    }

    ByteBuffer & position(uint32_t newPosition) {
        position_ = newPosition;
        return *this;
    }

    int8_t * getBuffer() {
        return p_buffer_;
    }

    void copyTo(char * buf, int32_t size) {
        memcpy(buf, &p_buffer_[position_], size);
    }

private:
    template<typename T>
    T read(uint32_t index) const {
        if (!p_buffer_ || index + sizeof(T) > limit_)
            return 0;

        return *((T *) &p_buffer_[index]);
    }

    template<typename T>
    T read() {
        T data = read<T>(position_);
        position_ += sizeof(T);
        return data;
    }

    template<typename T>
    void append(T data) {
        if (!p_buffer_)
            return;

        uint32_t s = sizeof(data);
        checkSize(s);

        memcpy(&p_buffer_[position_], (int8_t *) &data, s);
        position_ += s;
    }

    template<typename T>
    void insert(T data, uint32_t index) {
        uint32_t s = sizeof(data);
        checkSize(index, s);

        position_ = index;
        append<T>(data);
    }

    void checkSize(uint32_t increase) {
        checkSize(position_, increase);
    }

    void checkSize(uint32_t index, uint32_t increase) {
        if (index + increase <= capacity_)
            return;

        uint32_t newSize = capacity_ + (increase + BUFFER_SIZE_INCREASE - 1) /
                                       BUFFER_SIZE_INCREASE * BUFFER_SIZE_INCREASE;
        int8_t * pBuf = (int8_t *) realloc(p_buffer_, newSize);
        if (!pBuf) {
            throw std::runtime_error("realloc failed");
        }

        p_buffer_ = pBuf;
        capacity_ = newSize;
    }

private:
    const uint32_t BUFFER_SIZE_INCREASE = 2048;
    std::string name_;
    int8_t * p_buffer_;
    int32_t mark_;
    uint32_t limit_;
    uint32_t position_;
    uint32_t capacity_;
    bool alloc_buffer_;
};

#endif  /* _HDFS_LIBHDFS3_CLIENT_BYTEBUFFER_H_ */
