//
// Created by fortwoone on 14/09/2025.
//

#include "requests.hpp"
#include <iomanip>

using std::hex;

namespace cpp_kafka{
    void APIVersionArrEntry::append_to_response(Response& response) const{
        response.append(host_to_network_short(to_underlying(api_key))); // API key
        response.append(host_to_network_short(min_version));            // Min. supported version
        response.append(host_to_network_short(max_version));            // Max supported version
        response.append(static_cast<ubyte>(0));                         // Tag buffer
    }

    void TopicPartition::append_to_response(Response& response) const{
        response.append(host_to_network_short(to_underlying(err_code)));
        response.append(host_to_network_long(partition_index));
        response.append(host_to_network_long(leader_id));
        response.append(host_to_network_long(leader_epoch));
        response.append(static_cast<ubyte>(replica_nodes.size() + 1));
        for (const auto& rnode: replica_nodes){
            response.append(host_to_network_long(rnode));
        }
        response.append(static_cast<ubyte>(isr_nodes.size() + 1));
        for (const auto& rnode: isr_nodes){
            response.append(host_to_network_long(rnode));
        }
        response.append(static_cast<ubyte>(elr_nodes.size() + 1));
        for (const auto& rnode: elr_nodes){
            response.append(host_to_network_long(rnode));
        }
        response.append(static_cast<ubyte>(last_known_elr_nodes.size() + 1));
        for (const auto& rnode: last_known_elr_nodes){
            response.append(host_to_network_long(rnode));
        }
        response.append(static_cast<ubyte>(offline_replica_nodes.size() + 1));
        for (const auto& rnode: offline_replica_nodes){
            response.append(host_to_network_long(rnode));
        }
        response.append(static_cast<ubyte>(0));
    }

    void DescTopicPartArrEntry::append_to_response(Response& response) const{
        response.append(host_to_network_short(to_underlying(err_code)));    // Error code
        response.append(static_cast<fbyte>(topic_name.size() + 1));         // Topic name string length
        for (char c : topic_name){
            response.append(c);              // Topic name string
        }
        for (ubyte i: uuid){
            response.append(static_cast<ubyte>(host_to_network_short(i)));  // UUID portions
        }
        response.append(static_cast<fbyte>(is_internal ? 1 : 0));           // 1 if internal, 0 if not
        response.append(static_cast<fbyte>(partitions.size() + 1));         // Size of the partition array + 1 (because varint)
        for (const auto& obj: partitions){
            obj.append_to_response(response);
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


    vector<DescTopicPartArrEntry> retrieve_data(const vector<DescribeTopicReqArrEntry>& requested_topics){
        vector<DescTopicPartArrEntry> ret;
        ret.reserve(requested_topics.size());

        int cluster_metadata = open(METADATA_FILE_PATH, O_RDONLY);

        if (cluster_metadata == -1){
            throw runtime_error("Could not open the cluster metadata file. Please ensure it exists.");
        }

        ubyte buf[1024];
        ssize_t bytes_read = read(cluster_metadata, buf, 1024);
        close(cluster_metadata);

        if (bytes_read <= 0){
            throw runtime_error("Failed to read from the cluster metadata file.");
        }


        bool found = false;
        for (const auto& req_topic: requested_topics){
            string topic_name = req_topic.data;
            auto name_length = topic_name.length();
            for (ssize_t i = 0; i < bytes_read - name_length; ++i){
                if (!memcmp(buf + i, topic_name.c_str(), name_length) && !buf[i + name_length]){
                    found = true;
                    DescTopicPartArrEntry entry;
                    entry.topic_name = topic_name;
                    entry.err_code = KafkaErrorCode::NO_ERROR;

                    uint uuid_offset = i + name_length + 1;
                    if (uuid_offset + 16 <= bytes_read) {
                        cerr << "UUID: ";
                        for (ubyte j = 0; j < 16; ++j) {
                            entry.uuid[j] = buf[uuid_offset + j];
                            cerr << hex << static_cast<char>(buf[uuid_offset + j]);
                        }
                        cerr << "\n";

                        TopicPartition partition;
                        partition.err_code = KafkaErrorCode::NO_ERROR;
                        partition.partition_index = 0;
                        partition.leader_id = 1;
                        partition.leader_epoch = 0;
                        partition.replica_nodes = {1};
                        partition.isr_nodes = {1};
                        entry.partitions.push_back(partition);
                        ret.push_back(entry);
                    }
                }
            }
            if (!found){
                DescTopicPartArrEntry not_found;
                not_found.topic_name = topic_name;
                not_found.err_code = KafkaErrorCode::UNKNOWN_TOPIC_OR_PARTITION;
                not_found.uuid.fill(0);
                ret.push_back(not_found);
            }
            found = false;
        }

        return ret;
    }

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
        auto req_topic_arr_len = static_cast<fbyte>(read_big_endian<fbyte>(buffer + starting_point) - 1);
        requested_topics.resize(req_topic_arr_len);  // Create empty elements from the start so it is easier to edit them.

        uint offset_for_next_topic = 0;
        for (fbyte i = 0; i < req_topic_arr_len; ++i){
            auto string_name_length = read_big_endian<fbyte>(buffer + starting_point + offset_for_next_topic + 1);
            offset_for_next_topic++;
            auto& req_topic_obj = requested_topics.at(i);
            req_topic_obj.data.insert(
                req_topic_obj.data.end(),
                buffer + starting_point + offset_for_next_topic + 1,
                buffer + starting_point + offset_for_next_topic + 1 + string_name_length - 1
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
            vector<DescTopicPartArrEntry> topic_entries = retrieve_data(requested_topics);

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
