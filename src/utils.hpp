//
// Created by fortwoone on 14/09/2025.
//

#pragma once
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <netdb.h>
#include <string>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/types.h>

using std::cerr;
using std::cout;
using std::endl;
using std::memcpy;
using std::unitbuf;

// Macros defined so the code is more explicit.
// These two functions convert short and long values into big-endian format.
#define host_to_network_short(val) htons(val)
#define host_to_network_long(val) htonl(val)

using fbyte = int8_t;
using ubyte = uint8_t;
using fshort = int16_t;
using ushort = uint16_t;
using fint = int32_t;
using uint = uint32_t;

using InternetSockAddr = struct sockaddr_in;
using SockAddr = struct sockaddr;
using SockAddrPtr = struct sockaddr*;

namespace cpp_kafka{
    enum class KafkaAPIKey: fshort{
        API_VERSIONS = 18
    };

    enum class KafkaErrorCode: fshort{
        NO_ERROR = 0,
        UNSUPPORTED_VERSION = 35
    };
}
