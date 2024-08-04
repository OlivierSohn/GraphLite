
#include "Value.h"

#include <sstream>

template < typename > constexpr bool c_false = false;

std::ostream & operator <<(std::ostream& os, const Value & v)
{
  std::visit([&](auto && arg) {
    using T = std::decay_t<decltype(arg)>;
    if constexpr (std::is_same_v<T, int64_t>)
      os << arg;
    else if constexpr (std::is_same_v<T, double>)
      os << arg;
    else if constexpr (std::is_same_v<T, StringPtr>)
      os << arg.string.get();
    else if constexpr (std::is_same_v<T, ByteArrayPtr>)
      os << "<bytearray>";
    else if constexpr (std::is_same_v<T, Nothing>)
      os << "<null>";
    else
      static_assert(c_false<T>, "non-exhaustive visitor!");
  }, v);
  return os;
}

Value copy(Value const & v)
{
  return std::visit([&](auto && arg) {
    using T = std::decay_t<decltype(arg)>;
    if constexpr (std::is_same_v<T, int64_t>)
      return Value{arg};
    else if constexpr (std::is_same_v<T, double>)
      return Value{arg};
    else if constexpr (std::is_same_v<T, StringPtr>)
      return Value{arg.clone()};
    else if constexpr (std::is_same_v<T, ByteArrayPtr>)
      return Value{arg.clone()};
    else if constexpr (std::is_same_v<T, Nothing>)
      return Value{arg};
    else
      static_assert(c_false<T>, "non-exhaustive visitor!");
  }, v);
}

void append(Value && val, HomogeneousNonNullableValues & v)
{
  std::visit([&](auto && arg) {
    using T = std::decay_t<decltype(arg)>;
    if constexpr (std::is_same_v<T, int64_t>)
    {
      if(std::holds_alternative<std::monostate>(v))
        v = std::make_shared<std::vector<int64_t>>();
      std::get<std::shared_ptr<std::vector<int64_t>>>(v)->push_back(arg);
    }
    else if constexpr (std::is_same_v<T, double>)
    {
      if(std::holds_alternative<std::monostate>(v))
        v = std::make_shared<std::vector<double>>();
      std::get<std::shared_ptr<std::vector<double>>>(v)->push_back(arg);
    }
    else if constexpr (std::is_same_v<T, StringPtr>)
    {
      if(std::holds_alternative<std::monostate>(v))
        v = std::make_shared<Strings>();
      std::get<std::shared_ptr<Strings>>(v)->push_back(std::move(arg));
    }
    else if constexpr (std::is_same_v<T, ByteArrayPtr>)
    {
      if(std::holds_alternative<std::monostate>(v))
        v = std::make_shared<ByteArrays>();
      std::get<std::shared_ptr<ByteArrays>>(v)->push_back(std::move(arg));
    }
    else if constexpr (std::is_same_v<T, Nothing>)
      throw std::logic_error("list of null is not supported");
    else
      static_assert(c_false<T>, "non-exhaustive visitor!");
  }, val);
}

ByteArrayPtr ByteArrayPtr::clone() const
{
  if(bytes)
  {
    auto ptr = std::unique_ptr<unsigned char[]>{ new unsigned char[m_bufSz] };
    memcpy(ptr.get(), bytes.get(), m_bufSz);
    return ByteArrayPtr{std::move(ptr), m_bufSz};
  }
  return ByteArrayPtr{};
}

ByteArrayPtr ByteArrayPtr::fromByteArray(const void* b, const size_t sz)
{
  if(b)
  {
    auto ptr = std::unique_ptr<unsigned char[]>{ new unsigned char[sz] };
    memcpy(ptr.get(), b, sz);
    return ByteArrayPtr{std::move(ptr), sz};
  }
  return ByteArrayPtr{};
}

ByteArrayPtr ByteArrayPtr::fromHexStr(const std::string& str)
{
  std::vector<unsigned char> bytes;
  
  bytes.reserve(str.size() / 2);
  
  size_t i = 0;
  size_t end = str.size();
  
  // set i at the first hex digit.
  
  for(;;++i)
    if(std::isxdigit(str[i]))
      break;
  
  // set (end-1) at the last hex digit.
  
  for(;end > i;--end)
    if(std::isxdigit(str[end-1]))
      break;
  
  auto xdigitToInt = [](char xdigit) -> int
  {
    if(xdigit >= '0' && xdigit <= '9')
      return xdigit - '0';
    if(xdigit >= 'A' && xdigit <= 'F')
      return xdigit - 'A' + 10;
    if(xdigit >= 'a' && xdigit <= 'f')
      return xdigit - 'a' + 10;
    throw std::invalid_argument("Invalid input string");
  };
  
  for(;i<end;++i)
  {
    int val = 16 * xdigitToInt(str[i]);
    ++i;
    if(i >= end)
      throw std::invalid_argument("Invalid input string");
    val += xdigitToInt(str[i]);
    bytes.push_back(static_cast<unsigned char>(val));
  }
  
  return fromByteArray(bytes.data(), bytes.size());
}

std::string ByteArrayPtr::toHexStr() const
{
  std::ostringstream s;
  s << "x'";
  s << std::hex << std::uppercase;
  for(size_t i{}; i<m_bufSz; ++i)
  {
    const unsigned char uc = bytes[i];
    if(uc < 16)
      s << '0';
    s << static_cast<int>(uc);
  }
  s << "'";
  return s.str();
}


StringPtr StringPtr::fromCStrAndCountBytes(const unsigned char * s, const size_t sz)
{
  if(s)
  {
    const auto bufSz = sz;
    auto ptr = std::unique_ptr<char[]>{ new char[bufSz] };
    memcpy(ptr.get(), s, bufSz);
    return StringPtr{std::move(ptr), bufSz};
  }
  // Should never happen.
  return StringPtr{};
}

StringPtr StringPtr::fromCStr(const char * str)
{
  if(str)
  {
    const auto bufSz = strlen(str) + 1;
    auto ptr = std::unique_ptr<char[]>{ new char[bufSz] };
    memcpy(ptr.get(), str, bufSz);
    return StringPtr{std::move(ptr), bufSz};
  }
  // Should never happen.
  return StringPtr{};
}

StringPtr StringPtr::clone() const
{
  if(string)
  {
    auto ptr = std::unique_ptr<char[]>{ new char[m_bufSz] };
    memcpy(ptr.get(), string.get(), m_bufSz);
    return StringPtr{std::move(ptr), m_bufSz};
  }
  return StringPtr{};
}
