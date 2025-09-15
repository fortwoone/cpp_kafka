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

//    struct APIVersionArrEntry{
//        KafkaAPIKey api_key;
//        fshort min_version, max_version;
//        ubyte tag_buffer{0};
//    };

    struct RequestHeader{
        fshort request_api_key, request_api_version;
        fint correlation_id;
        string client_id;
    };

    class Request{
        RequestHeader header;

        public:
            Request() = default;

            [[nodiscard]] fshort get_api_key() const;
            [[nodiscard]] fint get_correlation_id() const;
            [[nodiscard]] fshort get_api_version() const;
            [[nodiscard]] string get_client_id() const;

            void set_api_key(fshort new_key);
            void set_api_version(fshort new_ver);
            void set_correlation_id(fint value);
    };

    class Response{
        fint msg_size;
        fint correlation_id;
        vector<ubyte> data;

        public:
            Response();

            [[nodiscard]] fint get_msg_size() const;
            [[nodiscard]] fint get_correlation_id() const;
            [[nodiscard]] vector<ubyte> get_body() const;

            template<class T> void append(const T& value){
                const auto* data_as_bytes = reinterpret_cast<const ubyte*>(&value);
                fint size = sizeof(T);
                data.insert(data.end(), data_as_bytes, data_as_bytes + size);
                msg_size += size;
            }

            void append(vector<ubyte> contents);

            void set_correlation_id(fint value);

            void send_to_client(int client_fd);
    };

    int receive_request_from_client(int client_fd, Response& response, Request& request);
}
