//
// Created by fortwoone on 17/09/2025.
//

#pragma once

#include "utils.hpp"
#include <stdexcept>
#include <type_traits>
#include <vector>


namespace cpp_kafka{
    using std::is_same_v;
    using std::is_unsigned_v;
    using std::runtime_error;
    using std::vector;

    class varint_t{
        fint value;  // The actual value in memory.

        public:
            // Ctor
            varint_t() = default;
            template <class IntType> varint_t(IntType val){  // NOLINT: constructor is intentionally left available for implicit conversion
                // VARINTS can only store up to 32 bits.
                static_assert(sizeof(val) < sizeof(flong), "Cannot use 64-bit integers for regular VARINTs. Use VARLONG instead.");
                value = static_cast<fint>(val);
            }

            // Equality operator
            template <class IntType> bool operator==(const IntType& val){
                return static_cast<flong>(val) == static_cast<flong>(value);
            }

            // Spaceship operator (C++20)
            template <class IntType> auto operator<=>(const IntType& val){
                return static_cast<flong>(value) <=> static_cast<flong>(val);
            }

            template<class IntType> explicit operator IntType(){
                return static_cast<IntType>(value);
            }

            template<class IntType> varint_t operator+(const IntType& val){
                static_assert(sizeof(IntType) < sizeof(ulong), "Cannot add varints to long integers due to size difference.");
                return varint_t(static_cast<fint>(val) + value);
            }

            template <class IntType> varint_t operator-(const IntType& val){
                static_assert(sizeof(IntType) < sizeof(ulong), "Cannot subtract longs from varints due to size difference.");
                return varint_t(value - static_cast<fint>(val));
            }

            template <class IntType> varint_t operator*(const IntType& val){
                static_assert(sizeof(IntType) < sizeof(ulong), "Cannot multiply varints with longs due to size difference.");
                return varint_t(value * static_cast<fint>(val));
            }

            template <class IntType> varint_t operator/(const IntType& val){
                static_assert(sizeof(IntType) < sizeof(ulong), "Cannot divide varints by longs due to size difference.");
                return varint_t(value / static_cast<fint>(val));
            }

            template <class IntType> varint_t operator>>(const IntType& val){
                return varint_t(value >> static_cast<fint>(val));
            }

            template <class IntType> varint_t operator<<(const IntType& val){
                return varint_t(value << static_cast<fint>(val));
            }

//            [[nodiscard]] size_t needed_size() const;
            [[nodiscard]] static varint_t decode_and_advance(char* buf, ssize_t& offset);
            [[nodiscard]] vector<ubyte> encode() const;
    };
    
    // UNSIGNED_VARINT type. Can be encoded to a variable size in responses and read from requests and log files.
    class unsigned_varint_t{
        uint value;  // The actual value in memory.

        public:
            // Ctor
            unsigned_varint_t() = default;
            template <class IntType> unsigned_varint_t(IntType val){  // NOLINT: constructor is intentionally left available for implicit conversion
                // VARINTS can only store up to 32 bits.
                static_assert(sizeof(val) < sizeof(flong), "Cannot use 64-bit integers for regular VARINTs. Use VARLONG instead.");
                value = static_cast<uint>(val);
            }

            // Equality operator
            template <class IntType> bool operator==(const IntType& val){
                return static_cast<ulong>(val) == static_cast<ulong>(value);
            }

            // Spaceship operator (C++20)
            template <class IntType> auto operator<=>(const IntType& val){
                return static_cast<ulong>(value) <=> static_cast<ulong>(val);
            }

            template<class IntType> explicit operator IntType(){
                return static_cast<IntType>(value);
            }

            template<class IntType> unsigned_varint_t operator+(const IntType& val){
                static_assert(sizeof(IntType) < sizeof(ulong), "Cannot add varints to long integers due to size difference.");
                return unsigned_varint_t(static_cast<uint>(val) + value);
            }

            template <class IntType> unsigned_varint_t operator-(const IntType& val){
                static_assert(sizeof(IntType) < sizeof(ulong), "Cannot subtract longs from varints due to size difference.");
                return unsigned_varint_t(value - static_cast<uint>(val));
            }

            template <class IntType> unsigned_varint_t operator*(const IntType& val){
                static_assert(sizeof(IntType) < sizeof(ulong), "Cannot multiply varints with longs due to size difference.");
                return unsigned_varint_t(value * static_cast<uint>(val));
            }

            template <class IntType> unsigned_varint_t operator/(const IntType& val){
                static_assert(sizeof(IntType) < sizeof(ulong), "Cannot divide varints by longs due to size difference.");
                return unsigned_varint_t(value / static_cast<uint>(val));
            }

            template <class IntType> unsigned_varint_t operator>>(const IntType& val){
                return unsigned_varint_t(value >> static_cast<uint>(val));
            }

            template <class IntType> unsigned_varint_t operator<<(const IntType& val){
                return unsigned_varint_t(value << static_cast<uint>(val));
            }

//            [[nodiscard]] size_t needed_size() const;
            [[nodiscard]] static unsigned_varint_t decode_and_advance(char* buf, ssize_t& offset);
            [[nodiscard]] vector<ubyte> encode() const;
    };
}
