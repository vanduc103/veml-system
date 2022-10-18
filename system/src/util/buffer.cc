// Copyright 2020 PIDL(Petabyte-scale In-memory Database Lab) http://kdb.snu.ac.kr

// Related include
#include "util/buffer.h"

namespace util {

/*
 * BufferReader
 */
template <>
BufferReader& operator>> (BufferReader& reader, EvalValue& value) {
    EvalType type;
    reader >> type;
    value.setEvalType(type);
    switch (type)
    {
        case ET_BIGINT:
        {
            BigInt v(0);
            reader >> v;
            value.getBigInt() = v;
            break;
        }
        case ET_DOUBLE:
        {
            Double v(0);
            reader >> v;
            value.getDouble() = v;
            break;
        }
        case ET_DECIMAL:
        {
            Decimal v(BigInt(0));
            reader >> v;
            value.getDecimal() = v;
            break;
        }
        case ET_STRING:
        {
            String v("");
            reader >> v;
            value.getString() = v;
            break;
        }
        case ET_BOOL:
        {
            Bool v(false);
            reader >> v;
            value.getBool() = v;
            break;
        }
        default:
            assert(false && "not supported value type");
    }

    return reader;
}

template <>
BufferReader& operator>> (BufferReader& reader, std::string& value) {
    int16_t length;
    reader >> length;

    value.assign(reader.buffer_, length);
    reader.buffer_ += length;
    reader.pos_ += length;
    return reader;
}

template <>
BufferReader& operator>> (BufferReader& reader, String& value) {
    int16_t length;
    reader >> length;

    value = String(reader.buffer_, length);
    reader.buffer_ += length;
    reader.pos_ += length;
    return reader;
}


/*
 * BufferWriter
 */
template <>
BufferWriter& operator<< (BufferWriter& writer, const EvalValue& value) {
    EvalType type = value.getEvalType();
    writer << type;
    writer.write(value);

    return writer;
}

template <>
BufferWriter& operator<< (BufferWriter& writer, const std::string& value) {
    int16_t length = value.length();
    writer << length;
    const char* begin = value.c_str();
    const char* end = begin + length;
    std::copy(begin, end, std::back_inserter(*writer.buffer_.get()));
    return writer;
}

template <>
BufferWriter& operator<< (BufferWriter& writer, const String& value) {
    int16_t length = value.size();
    writer << length;
    const char* begin = value.ptr();
    const char* end = begin + length;
    std::copy(begin, end, std::back_inserter(*writer.buffer_.get()));
    return writer;
}

template <>
void BufferWriter::write(const EvalValue& val, bool endian) {
    EvalType type = val.getEvalType();

    switch (type)
    {
        case ET_BIGINT:
            (*this).write(val.getBigIntConst().get(), endian);
            break;
        case ET_DOUBLE:
            (*this).write(val.getDoubleConst().get(), endian);
            break;
        case ET_DECIMAL:
            (*this).write(val.getDecimalConst().get(), endian);
            break;
        case ET_STRING:
            (*this).write(val.getStringConst(), endian);
            break;
        case ET_BOOL:
            (*this).write(val.getBoolConst(), endian);
            break;
        case ET_DATE:
            (*this).write(val.getDateConst(), endian);
            break;
        default:
            assert(false && "not supported value type");
    }
}

template <>
void BufferWriter::write(const std::string& value, bool endian) {
    int16_t length = value.length();
    (*this).write(length, endian);
    const char* begin = value.c_str();
    const char* end = begin + length;
    std::copy(begin, end, std::back_inserter(*buffer_.get()));
}

template <>
void BufferWriter::write(const String& value, bool endian) {
    int16_t length = value.size();
    (*this).write(length, endian);
    const char* begin = value.ptr();
    const char* end = begin + length;
    std::copy(begin, end, std::back_inserter(*buffer_.get()));
}

template <>
void BufferWriter::write(const int64_t& val, bool endian) {
    if (endian) {
        size_t size = sizeof(int64_t);
        char buf[size];
        for (unsigned i = 0; i < size; i++) {
            buf[i] = *((char*)(&val) + size - i - 1);
        }
        std::copy(buf, buf + size, std::back_inserter(*buffer_.get()));
    } else {
        (*this) << val;
    }
}

template <>
void BufferWriter::write(const int32_t& val, bool endian) {
    if (endian) {
        size_t size = sizeof(int32_t);
        char buf[size];
        for (unsigned i = 0; i < size; i++) {
            buf[i] = *((char*)(&val) + size - i - 1);
        }
        std::copy(buf, buf + size, std::back_inserter(*buffer_.get()));
    } else {
        (*this) << val;
    }
}

template <>
void BufferWriter::write(const int16_t& val, bool endian) {
    if (endian) {
        size_t size = sizeof(int16_t);
        char buf[size];
        for (unsigned i = 0; i < size; i++) {
            buf[i] = *((char*)(&val) + size - i - 1);
        }
        std::copy(buf, buf + size, std::back_inserter(*buffer_.get()));
    } else {
        (*this) << val;
    }
}

template <>
void BufferWriter::write(const double& val, bool endian) {
    if (endian) {
        size_t size = sizeof(double);
        char buf[size];
        for (unsigned i = 0; i < size; i++) {
            buf[i] = *((char*)(&val) + size - i - 1);
        }
        std::copy(buf, buf + size, std::back_inserter(*buffer_.get()));
    } else {
        (*this) << val;
    }
}

template <>
void BufferWriter::write(const float& val, bool endian) {
    if (endian) {
        size_t size = sizeof(float);
        char buf[size];
        for (unsigned i = 0; i < size; i++) {
            buf[i] = *((char*)(&val) + size - i - 1);
        }
        std::copy(buf, buf + size, std::back_inserter(*buffer_.get()));
    } else {
        (*this) << val;
    }
}

void BufferWriter::writeBytes(PhysicalPtr ptr, int32_t len) {
    std::copy(ptr, ptr + len, std::back_inserter(*buffer_.get()));
}

}  // namespace util
