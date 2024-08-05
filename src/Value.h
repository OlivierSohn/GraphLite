/*
 Copyright 2024-present Olivier Sohn
 
 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at
 
 http://www.apache.org/licenses/LICENSE-2.0
 
 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

#pragma once

#include <variant>
#include <memory>
#include <cstring>
#include <exception>
#include <ostream>

#ifdef _WIN32
struct iovec {
  void *iov_base;
  size_t iov_len;
};
#else
# include <sys/uio.h>
#endif


enum class ValueType
{
  ByteArray,
  String,
  Integer,
  Float
};

std::string toStr(ValueType);

struct Nothing{
};

// TODO: provide a SmallString type for strings up to 7 characters .

// UTF8 string
struct StringPtr{
  StringPtr() = default;

  StringPtr(std::unique_ptr<char[]> && s, size_t bufSz)
  : string(std::move(s))
  , m_bufSz(bufSz)
  {}

  StringPtr(StringPtr&&) noexcept = default;
  StringPtr& operator=(StringPtr&&) noexcept = default;

  // null terminated
  std::unique_ptr<char[]> string;
  size_t m_bufSz{};
  
  static StringPtr fromCStr(const char * str);
  static StringPtr fromCStrAndCountBytes(const unsigned char * s, const size_t sz);
  
  StringPtr clone() const;

  friend bool operator == (const StringPtr& a, const StringPtr& b);
};

namespace std
{
template<>
struct hash<StringPtr>
{
  size_t operator()(const StringPtr& s) const
  {
    return std::hash<std::string_view>()({s.string.get(), s.string.get() + s.m_bufSz});
  }
};
}


struct ByteArrayPtr{
  ByteArrayPtr() = default;

  ByteArrayPtr(std::unique_ptr<unsigned char[]> && b, size_t bufSz)
  : bytes(std::move(b))
  , m_bufSz(bufSz)
  {}

  ByteArrayPtr(ByteArrayPtr&&) noexcept = default;
  ByteArrayPtr& operator=(ByteArrayPtr&&) noexcept = default;

  std::unique_ptr<unsigned char[]> bytes;
  size_t m_bufSz{};
  

  static ByteArrayPtr fromByteArray(const void* b, const size_t sz);
  static ByteArrayPtr fromHexStr(const std::string& str);
  std::string toHexStr() const;

  ByteArrayPtr clone() const;

  friend bool operator == (const ByteArrayPtr& a, const ByteArrayPtr& b);
};


namespace std
{
template<>
struct hash<ByteArrayPtr>
{
  size_t operator()(const ByteArrayPtr& s) const
  {
    std::size_t seed = s.m_bufSz;
    for(size_t i{}; i<s.m_bufSz; ++i)
      seed ^= s.bytes[i] + 0x9e3779b9 + (seed << 6) + (seed >> 2);
    return seed;
  }
};
}


// using StringPtr results in a variant of size 16
// using std::string results in a variant of size 32 (and may depend on the C++ library used)
using Value = std::variant<Nothing, double, int64_t, StringPtr, ByteArrayPtr>;

Value copy(Value const & v);

std::ostream & operator <<(std::ostream& os, const Value & v);

struct ByteArrays
{
  void reserve(size_t sz)
  {
    arrays.reserve(sz);
    iovecs.reserve(sz);
  }
  void push_back(ByteArrayPtr && v)
  {
    iovecs.push_back(iovec{v.bytes.get(), v.m_bufSz});
    arrays.push_back(std::move(v.bytes));
  }
  std::vector<std::unique_ptr<unsigned char[]>> arrays;
  std::vector<iovec> iovecs; // this is used for binding via carray sqlite extention.
};

struct Strings
{
  void reserve(size_t sz)
  {
    strings.reserve(sz);
    stringsArray.reserve(sz);
  }
  void push_back(StringPtr && v)
  {
    stringsArray.push_back(v.string.get());
    strings.push_back(std::move(v.string));
  }
  std::vector<std::unique_ptr<char[]>> strings;
  std::vector<char*> stringsArray; // this is used for binding via the carray sqlite extention.
};

using HomogeneousNonNullableValues = std::variant<
std::monostate, // empty list
std::shared_ptr<std::vector<double>>,
std::shared_ptr<std::vector<int64_t>>,
std::shared_ptr<Strings>, // this format is OK for binding via the carray sqlite extention.
std::shared_ptr<ByteArrays>
>;

template<typename Value_T>
struct CorrespondingVectorType;

template<>
struct CorrespondingVectorType<double>
{
  using type = std::vector<double>;
};
template<>
struct CorrespondingVectorType<int64_t>
{
  using type = std::vector<int64_t>;
};
template<>
struct CorrespondingVectorType<StringPtr>
{
  using type = Strings;
};
template<>
struct CorrespondingVectorType<ByteArrayPtr>
{
  using type = ByteArrays;
};


// Will throw if v has a value and val is incompatible with this value.
void append(Value && val, HomogeneousNonNullableValues & v);


template<typename T>
struct Traits;

template<>
struct Traits<int64_t>
{
  static constexpr auto correspondingValueType = ValueType::Integer;
};
template<>
struct Traits<double>
{
  static constexpr auto correspondingValueType = ValueType::Float;
};
template<>
struct Traits<StringPtr>
{
  static constexpr auto correspondingValueType = ValueType::String;
};
template<>
struct Traits<ByteArrayPtr>
{
  static constexpr auto correspondingValueType = ValueType::ByteArray;
};

