/*
 * This file is subject to this repository: https://github.com/CPythoner/ByteBuffer.
 * We made some changes and optimizations.
 */
#ifndef _HDFS_LIBHDFS3_CLIENT_BYTEBUFFER_H_
#define _HDFS_LIBHDFS3_CLIENT_BYTEBUFFER_H_

#include <cstdlib>
#include <string>
#include <iostream>
#include <cstdint>

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
        if (alloc_buffer_) {
            p_buffer_ = static_cast<int8_t *>(calloc(capacity_, sizeof(int8_t)));
        }
    }

    ByteBuffer(int8_t * arr, uint32_t length, const char * name = "")
            : name_(name), 
              mark_(-1),
              limit_(length),
              position_(0),
              capacity_(length) {
        p_buffer_ = static_cast<int8_t *>(calloc(capacity_, sizeof(int8_t)));

        putInt8_ts(arr, capacity_);
        clear();
    }

    ~ByteBuffer() {
        if (alloc_buffer_ && p_buffer_) {
            free(p_buffer_);
        }
        p_buffer_ = nullptr;
    }

    // Write Methods
    ByteBuffer & put(const ByteBuffer * bb) {
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

    inline void putInt8_ts(const int8_t * buf, uint32_t len) {
        if (!p_buffer_)
            return;
        checkSize(len);
        memcpy(&p_buffer_[position_], buf, len);
        position_ += len;
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

    float getFloat() {
        return read<float>();
    }

    double getDouble() {
        return read<double>();
    }

    ByteBuffer * duplicate() {
        ByteBuffer * newBuffer = new ByteBuffer(capacity_, "", false);

        newBuffer->p_buffer_ = p_buffer_;

        newBuffer->limit(limit_);
        newBuffer->position(position_);

        return newBuffer;
    }

    ByteBuffer * slice() {
        ByteBuffer *newBuffer = new ByteBuffer(remaining(), "", false);

        newBuffer->p_buffer_ = p_buffer_ + position_;

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

    ByteBuffer & reset() {
        if (mark_ >= 0)
            position_ = mark_;

        return *this;
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

        if (mark_ > static_cast<int32_t>(newLimit))
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
        int8_t * pBuf = static_cast<int8_t *>(realloc(p_buffer_, newSize));
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
