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

