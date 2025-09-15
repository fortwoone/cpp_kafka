//
// Created by fortwoone on 14/09/2025.
//

#pragma once
#include <array>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "utils.hpp"

namespace cpp_kafka{
    using std::array;
    using std::runtime_error;
    using std::string;
    using std::to_underlying;
    using std::vector;

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

    struct APIVersionArrEntry{
        KafkaAPIKey api_key;
        fshort min_version, max_version;
        ubyte tag_buffer{0};

        void append_to_response(Response& response) const;
    };

    // A topic as it is sent in a DescribeTopicPartitions request.
    struct DescribeTopicReqArrEntry{
        string data;
    };

    struct TopicUUID{
        // Force UUID parts to be stored contiguously in memory.
        // Doing this because relying on __int128 is generally a bad idea
        // (not a standard type, and as such most standard functions aren't compatible with it).
        array<ulong, 2> uuid_portions;
    };

    struct TopicPartition{
        // Empty for now: only supporting "unknown topic" responses
    };

    enum TopicOperationFlags: uint{
        UNKNOWN = 0,
        ANY = 1,
        ALL = (1 << 2),
        READ = (1 << 3),
        WRITE = (1 << 4),
        CREATE = (1 << 5),
        DELETE = (1 << 6),
        ALTER = (1 << 7),
        DESCRIBE = (1 << 8),
        CLUSTER_ACTION = (1 << 9),
        DESCRIBE_CONFIGS = (1 << 10),
        ALTER_CONFIGS = (1 << 11),
        IDEMPOTENT_WRITE = (1 << 12),
        CREATE_TOKENS = (1 << 13),
        DESCRIBE_TOKENS = (1 << 14)
    };

    struct DescTopicPartArrEntry{
        KafkaErrorCode err_code;
        string topic_name;
        TopicUUID uuid;
        bool is_internal;
        vector<TopicPartition> partitions;
        TopicOperationFlags allowed_ops_flags;

        void append_to_response(Response& response) const;
    };

    void handle_api_versions_request(const Request& request, Response& response);
    void handle_describe_topic_partitions_request(const Request& request, Response& response, char* buffer);

    int receive_request_from_client(int client_fd, Response& response, Request& request);
}
