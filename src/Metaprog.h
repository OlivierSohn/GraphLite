
#pragma once

#include <variant>

// Whether type T is a member of the std::variant Variant.
template<typename T, typename Variant>
struct isVariantMember;

template<typename T, typename... TypesInVariant>
struct isVariantMember<T, std::variant<TypesInVariant...>>
: public std::disjunction<std::is_same<T, TypesInVariant>...> {};


template <typename T, typename...U>
using is_all_same = std::integral_constant<bool, (... && std::is_same_v<T,U>)>;

// This is useful to initialize a vector with some non move-only elements.
//
template<typename T, std::size_t N>
auto mkVec( std::array<T,N>&& a )
-> std::vector<T>
{
  return { std::make_move_iterator(std::begin(a)),
    std::make_move_iterator(std::end(a)) };
}
template<typename U, typename... T>
auto mkVec( U&&u, T&& ... t ) -> std::enable_if_t<is_all_same<U, T...>::value, std::vector<U>>
{
  return mkVec( std::to_array({ std::forward<U>(u), std::forward<T>(t)... }) );
}

