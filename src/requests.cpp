//
// Created by fortwoone on 14/09/2025.
//

#include "requests.hpp"

namespace cpp_kafka{
    void APIVersionArrEntry::append_to_response(Response& response) const{
        response.append(host_to_network_short(to_underlying(api_key))); // API key
        response.append(host_to_network_short(min_version));            // Min. supported version
        response.append(host_to_network_short(max_version));            // Max supported version
        response.append(static_cast<ubyte>(0));                         // Tag buffer
    }

    void DescTopicPartArrEntry::append_to_response(Response& response) const{
        response.append(host_to_network_short(to_underlying(err_code)));    // Error code
        cerr << "Appending " << sizeof(err_code) << "bytes to response\n";
        response.append(static_cast<fbyte>(topic_name.size() + 1));         // Topic name string length
        cerr << "Appending 1 byte to response\n";
        for (char c: topic_name){
            response.append(c);                                             // Topic name string
        }
        response.append(static_cast<ulong>(host_to_network_long(uuid.uuid_portions[0])));       // Higher part of the UUID
        response.append(static_cast<ulong>(host_to_network_long(uuid.uuid_portions[1])));       // Lower part of the UUID
        response.append(static_cast<fbyte>(is_internal ? 1 : 0));           // 1 if internal, 0 if not
        response.append(static_cast<fbyte>(partitions.size() + 1));         // Size of the partition array + 1 (because varint)
        for (const auto& obj: partitions){
            // Do nothing for now. We'll handle this later.
        }
        response.append(host_to_network_long(allowed_ops_flags));           // Allowed operations bitfield
        response.append(static_cast<ubyte>(0));                             // Tag buffer
    }

    // region Request
    fshort Request::get_api_key() const{
        return header.request_api_key;
    }

    fshort Request::get_api_version() const{
        return header.request_api_version;
    }

    fint Request::get_correlation_id() const{
        return header.correlation_id;
    }

    string Request::get_client_id() const{
        return header.client_id;
    }

    void Request::set_api_key(fshort new_key){
        header.request_api_key = new_key;
    }

    void Request::set_api_version(fshort new_ver){
        header.request_api_version = new_ver;
    }

    void Request::set_correlation_id(fint value){
        header.correlation_id = value;
    }
    // endregion

    // region Response
    Response::Response(){
        msg_size = sizeof(correlation_id);
        correlation_id = 0;
    }

    fint Response::get_msg_size() const{
        return msg_size;
    }

    fint Response::get_correlation_id() const{
        return correlation_id;
    }

    vector<ubyte> Response::get_body() const{
        return data;
    }

    void Response::append(vector<ubyte> contents){
        data.insert(data.end(), contents.begin(), contents.end());
        msg_size += static_cast<fint>(contents.size());
    }

    void Response::set_correlation_id(fint value){
        correlation_id = value;
    }

    void Response::send_to_client(int client_fd){
        fint msg_size_as_net = host_to_network_long(msg_size);

        ssize_t bytes_written = send(client_fd, &msg_size_as_net, sizeof(msg_size_as_net), 0);
        if (bytes_written <= 0){
            throw runtime_error("Could not send message size to client.");
        }

        fint corr_id_as_net = host_to_network_long(correlation_id);
        bytes_written = send(client_fd, &corr_id_as_net, sizeof(corr_id_as_net), 0);
        if (bytes_written <= 0){
            throw runtime_error("Failed to send the correlation ID to the client.");
        }

        const auto& body = data;
        bytes_written = send(client_fd, body.data(), body.size(), 0);
        if (bytes_written <= 0){
            throw runtime_error("Could not send response body to client.");
        }
    }
    // endregion


    void handle_api_versions_request(const Request& request, Response& response){
        fshort version = request.get_api_version();
        if (version >= 0 && version <= 4){
            vector<APIVersionArrEntry> version_entries;
            version_entries.reserve(2);
            version_entries.push_back(
                {
                    KafkaAPIKey::API_VERSIONS,
                    0,
                    4
                }
            );
            version_entries.push_back(
                {
                    KafkaAPIKey::DESCRIBE_TOPIC_PARTITIONS,
                    0,
                    0
                }
            );

            response.append(host_to_network_short(to_underlying(KafkaErrorCode::NO_ERROR)));    // Error code
            response.append(static_cast<ubyte>(version_entries.size() + 1));                    // Length of API version entries + 1

            // Append all entries to the response.
            for (const auto& ver: version_entries){
                ver.append_to_response(response);
            }

            response.append(host_to_network_long(0));   // Throttle time (ms)
            response.append(static_cast<ubyte>(0));     // Tag buffer for throttle field.
        }
        else{
            response.append(host_to_network_short(to_underlying(KafkaErrorCode::UNSUPPORTED_VERSION))); // Error code
        }
    }

    void handle_describe_topic_partitions_request(const Request& request, Response& response, char* buffer){
        auto cli_id_size = read_big_endian<fshort>(buffer + 12);

        // Calculation details:
        // 12 bytes from the previous header fields + 2 bytes from the string length + 1 byte for the empty tag buffer = 15 bytes
        // Then we add the size of the string itself, and this determines where to start parsing the body in the message.
        uint starting_point = 15 + cli_id_size;

        // Extract the requested topics.
        vector<DescribeTopicReqArrEntry> requested_topics;
        // Since this value is encoded as a VARINT, we need to subtract 1 from this count.
        fbyte req_topic_arr_len = read_big_endian<fbyte>(buffer + starting_point) - 1;
        requested_topics.resize(req_topic_arr_len);  // Create empty elements from the start so it is easier to edit them.

        uint offset_for_next_topic = 0;
        for (fbyte i = 0; i < req_topic_arr_len; ++i){
            fbyte string_name_length = read_big_endian<fbyte>(buffer + starting_point + offset_for_next_topic + 1);
            offset_for_next_topic++;
            auto& req_topic_obj = requested_topics.at(i);
            req_topic_obj.data.insert(
                req_topic_obj.data.end(),
                buffer + starting_point + offset_for_next_topic + 1,
                buffer + starting_point + offset_for_next_topic + 1 + string_name_length
            );
            // Account for the tag buffer (one empty byte).
            offset_for_next_topic += string_name_length + 1;
        }

        auto max_part_count_pos = starting_point + offset_for_next_topic;

        auto max_part_count = read_big_endian<fint>(buffer + max_part_count_pos);
        auto cursor = read_big_endian<fbyte>(buffer + max_part_count_pos + sizeof(fint));


        fshort version = request.get_api_version();
        response.append(static_cast<ubyte>(0)); // Tag buffer
        if (version > 0){
            response.append(host_to_network_short(to_underlying(KafkaErrorCode::UNSUPPORTED_VERSION))); // Error code
        }
        else{
            vector<DescTopicPartArrEntry> topic_entries;
            topic_entries.resize(requested_topics.size());
            for (size_t i = 0; i < topic_entries.size(); ++i){
                const auto& req_topic = requested_topics.at(i);
                auto& resp_topic = topic_entries.at(i);
                resp_topic.err_code = KafkaErrorCode::UNKNOWN_TOPIC_OR_PARTITION;
                resp_topic.topic_name = req_topic.data;
                resp_topic.uuid.uuid_portions.fill(0);  // set a null UUID for now
                resp_topic.is_internal = false;
                resp_topic.allowed_ops_flags = TopicOperationFlags::UNKNOWN;
            }

            response.append(static_cast<fint>(0));                          // Throttle time
            response.append(static_cast<fbyte>(topic_entries.size() + 1));  // Topic array size + 1 (because varint)
            for (const auto& topic: topic_entries){
                topic.append_to_response(response);                         // Append results for each topic to the response.
            }
            response.append(static_cast<ubyte>(0xFF));                      // Next cursor (null for now)
            response.append(static_cast<ubyte>(0));                         // Tag buffer
        }
    }

    int receive_request_from_client(int client_fd, Response& response, Request& request){
        char buffer[1024];
        ssize_t bytes_read = recv(client_fd, buffer, sizeof(buffer), 0);
        if (bytes_read <= 0){
            cerr << "Couldn't read request, or client disconnected\n";
            return 1;
        }

        // Extract the request API key from the buffer.
        request.set_api_key(read_big_endian<fshort>(buffer + 4));

        cerr << "Request API Key: " << request.get_api_key() << "\n";

        // Extract the request API version from the buffer.
        request.set_api_version(read_big_endian<fshort>(buffer + 6));

        cerr << "Request API Version: " << request.get_api_version() << "\n";

        // Extract correlation ID from the buffer.
        response.set_correlation_id(read_big_endian<fint>(buffer + 8));
        request.set_correlation_id(response.get_correlation_id());

        cerr << "Request Correlation ID: " << response.get_correlation_id() << "\n";

        switch ((KafkaAPIKey)request.get_api_key()){
            case KafkaAPIKey::API_VERSIONS:
                handle_api_versions_request(request, response);
                break;
            case KafkaAPIKey::DESCRIBE_TOPIC_PARTITIONS:
                handle_describe_topic_partitions_request(request, response, buffer);
                break;
            default:
                break;
        }

        return 0;
    }
}
