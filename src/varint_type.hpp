//
// Created by fortwoone on 17/09/2025.
//

#pragma once

#include "utils.hpp"
#include <type_traits>


namespace cpp_kafka{
    using std::is_same_v;
    using std::is_unsigned_v;

    // VARINT type. Can be encoded to a variable size in responses and read from requests and log files.
    class varint_t{
        uint value;  // The actual value in memory.

        // Ctor
        template<class IntType> varint_t(IntType val){
            // VARINTS can only store up to 32 bits.
            static_assert(sizeof(val) < sizeof(flong), "Cannot use 64-bit integers for regular VARINTs. Use VARLONG instead.");
            value = static_cast<uint>(val);
        }

        // Comparison operator

    };
}
