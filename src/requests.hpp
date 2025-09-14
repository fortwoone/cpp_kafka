//
// Created by fortwoone on 14/09/2025.
//

#pragma once
#include <stdexcept>
#include <utility>

#include "utils.hpp"

namespace cpp_kafka{
    using std::runtime_error;
    using std::to_underlying;

    struct RequestHeader{
        fshort request_api_key, request_api_version;
        fint correlation_id;
    };

    struct Response{
        fint msg_size;
        RequestHeader header;
        KafkaErrorCode err_code;

        void send_to_client(int client_fd);
    };

    int receive_request_from_client(int client_fd, Response& request);
}
