//
// Created by fortwoone on 14/09/2025.
//

#pragma once
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "utils.hpp"

namespace cpp_kafka{
    using std::runtime_error;
    using std::string;
    using std::to_underlying;
    using std::vector;

    struct APIVersionArrEntry{
        KafkaAPIKey api_key;
        fshort min_version, max_version;
        ubyte tag_buffer{0};
    };

    struct RequestHeader{
        fshort request_api_key, request_api_version;
        fint correlation_id;
        string client_id;
    };

    struct Request{
        RequestHeader header;
    };

    struct Response{
        fint msg_size;
        fint correlation_id;
        KafkaErrorCode err_code;
        vector<APIVersionArrEntry> api_versions;
        fint throttle_time_ms;

        void send_to_client(int client_fd);
    };

    int receive_request_from_client(int client_fd, Response& response, Request& request);
}
