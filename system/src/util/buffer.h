// Copyright 2020 PIDL(Petabyte-scale In-memory Database Lab) http://kdb.snu.ac.kr

#ifndef UTIL_BUFFER_H_
#define UTIL_BUFFER_H_

// Project include
#include "common/types/eval_type.h"

// Other include
#include <boost/shared_ptr.hpp>
#include <boost/make_shared.hpp>

namespace util {

typedef std::vector<char>           buffer_t;
typedef boost::shared_ptr<buffer_t> buffer_ref;

class BufferReader
{
  public:
    BufferReader(const char* buffer) : buffer_(buffer), pos_(0), size_(strlen(buffer)) {}
    BufferReader(const char* buffer, int64_t size) : buffer_(buffer), pos_(0), size_(size) {}
    BufferReader(const BufferReader& other)
        : buffer_(other.buffer_), pos_(other.pos_), size_(other.size_) {}

    operator bool()     { return pos_ != size_; }

    template <typename T>
    friend BufferReader& operator>> (BufferReader& reader, T& value);

    template <typename T>
    friend BufferReader& operator>> (BufferReader& reader, std::vector<T>& value);

  private:
    const char* buffer_;
    int64_t     pos_;
    int64_t     size_;
};

class BufferWriter
{
  public:
    BufferWriter() : buffer_(boost::make_shared<buffer_t>()) {}

    template <typename T>
    friend BufferWriter& operator<< (BufferWriter& writer, const T& value); 

    template <typename T>
    friend BufferWriter& operator<< (BufferWriter& writer, const std::vector<T>& value); 

    template <typename T>
    void write(const T& value, bool endian = false);

    void writeBytes(PhysicalPtr ptr, int32_t len);

    size_t getSize()        { return buffer_->size(); }
    const char* getData()   { return buffer_->data(); }

  private:
    buffer_ref buffer_;
};

/*
 * BufferReader
 */
template <typename T>
BufferReader& operator>> (BufferReader& reader, T& value) {
    value = *reinterpret_cast<const T*>(reader.buffer_);
    reader.buffer_ += sizeof(T);
    reader.pos_ += sizeof(T);
    return reader;
}
template <typename T>
BufferReader& operator>> (BufferReader& reader, std::vector<T>& value) {
    size_t length;
    reader >> length;
    for (unsigned i = 0; i < length; i++) {
        T v;
        reader >> v;
        value.push_back(v);
    }
    return reader;
}
template <>
inline BufferReader& operator>> (BufferReader& reader, EvalValue& value);
template <>
inline BufferReader& operator>> (BufferReader& reader, std::string& value);
template <>
inline BufferReader& operator>> (BufferReader& reader, String& value);

/*
 * BufferWriter
 */
template <typename T>
inline BufferWriter& operator<< (BufferWriter& writer, const T& value) {
    const char* begin = reinterpret_cast<const char*>(&value);
    const char* end = begin + sizeof(value);
    std::copy(begin, end, std::back_inserter(*writer.buffer_.get()));
    return writer;
}

template <typename T>
inline BufferWriter& operator<< (BufferWriter& writer, const std::vector<T>& value) {
    size_t length = value.size();
    writer << length;

    for (unsigned i = 0; i < length; i++) {
        writer << value[i];
    }
    return writer;
}

template <typename T>
inline void BufferWriter::write(const T& value, bool endian) {
    (*this) << value;
}

template <>
inline BufferWriter& operator<< (BufferWriter& writer, const EvalValue& value);
template <>
inline BufferWriter& operator<< (BufferWriter& writer, const std::string& value);
template <>
inline BufferWriter& operator<< (BufferWriter& writer, const String& value);

template <>
inline void BufferWriter::write(const EvalValue& value, bool endian);
template <>
inline void BufferWriter::write(const std::string& value, bool endian);
template <>
inline void BufferWriter::write(const String& value, bool endian);
template <>
inline void BufferWriter::write(const int64_t& value, bool endian);
template <>
inline void BufferWriter::write(const int32_t& value, bool endian);
template <>
inline void BufferWriter::write(const int16_t& value, bool endian);
template <>
inline void BufferWriter::write(const double& value, bool endian);
template <>
inline void BufferWriter::write(const float& value, bool endian);

}  // namespace util

#endif  // UTIL_BUFFER_H_
