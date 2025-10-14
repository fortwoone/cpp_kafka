//
// Created by fortwoone on 14/09/2025.
//

#include "requests.hpp"
#include <iostream>
#include <iomanip>
using std::cerr;

namespace cpp_kafka{
    // Loaded once so every child process can read it later
    static vector<RecordBatch> metadata = load_cluster_metadata();

    void APIVersionArrEntry::append_to_response(Response& response) const{
        response.append(host_to_network_short(to_underlying(api_key))); // API key
        response.append(host_to_network_short(min_version));            // Min. supported version
        response.append(host_to_network_short(max_version));            // Max supported version
        response.append(static_cast<ubyte>(0));                         // Tag buffer
    }

    void TopicPartition::append_to_response(Response& response) const{
        response.append(host_to_network_short(to_underlying(err_code)));                // Error code
        response.append(host_to_network_long(partition_index));                         // Partition index
        response.append(host_to_network_long(leader_id));                               // Partition leader ID
        response.append(host_to_network_long(leader_epoch));                            // Partition leader epoch
        varint_t repl_node_size = static_cast<fint>(replica_nodes.size() + 1);          // Partition replica node array length + 1 (varint)
        for (const ubyte& len_portion: repl_node_size.encode()){
            response.append(len_portion);                                               // Append portions of size
        }
        for (const auto& rnode: replica_nodes){
            response.append(host_to_network_long(rnode));                               // Replica nodes
        }
        varint_t isr_arr_size = static_cast<fint>(isr_nodes.size() + 1);                // Partition ISR node array length + 1 (varint)
        for (const ubyte& len_portion: isr_arr_size.encode()){
            response.append(len_portion);
        }
        for (const auto& rnode: isr_nodes){
            response.append(host_to_network_long(rnode));                               // ISR nodes
        }
        varint_t elr_arr_size = static_cast<fint>(elr_nodes.size() + 1);                // Partition ELR node array length + 1 (varint)
        for (const ubyte& len_portion: elr_arr_size.encode()){
            response.append(len_portion);
        }
        for (const auto& rnode: elr_nodes){
            response.append(host_to_network_long(rnode));                               // ELR nodes
        }
        varint_t lkelr_arr_size = static_cast<fint>(last_known_elr_nodes.size() + 1);   // Partition last known ELR array length + 1 (varint)
        for (const ubyte& len_portion: lkelr_arr_size.encode()){
            response.append(len_portion);
        }
        for (const auto& rnode: last_known_elr_nodes){
            response.append(host_to_network_long(rnode));                               // Last known ELR nodes
        }
        varint_t offrep_arr_size = static_cast<fint>(offline_replica_nodes.size() + 1); // Partition offline replica node array length + 1 (varint)
        for (const ubyte& len_portion: offrep_arr_size.encode()){
            response.append(len_portion);
        }
        for (const auto& rnode: offline_replica_nodes){
            response.append(host_to_network_long(rnode));                       // Offline replica nodes
        }
        response.append(static_cast<ubyte>(0));                                 // Tag buffer
    }

    void Topic::append_to_response(Response& response) const{
        response.append(host_to_network_short(to_underlying(err_code)));            // Error code
        varint_t topic_name_size = static_cast<fint>(topic_name.size() + 1);        // Topic name string length + 1 because array size encoding
        for (const ubyte& len_portion: topic_name_size.encode()){
            response.append(len_portion);                                           // Append portions of the varint
        }
        for (char c : topic_name){
            response.append(c);                                                     // Topic name string
        }
        for (ubyte i: uuid){
            response.append(static_cast<ubyte>(i));                                 // UUID portions
        }
        response.append(static_cast<fbyte>(is_internal ? 1 : 0));                   // 1 if internal, 0 if not
        varint_t partition_list_size = static_cast<fint>(partitions.size() + 1);    // Size of the partition array + 1 (because varint)
        for (const ubyte& part_len_portion: partition_list_size.encode()){
            response.append(part_len_portion);                                      // Append portions of the varint
        }
        for (const auto& obj: partitions){
            obj.append_to_response(response);                                       // Append all partitions
        }
        response.append(host_to_network_long(allowed_ops_flags));                   // Allowed operations bitfield
        response.append(static_cast<ubyte>(0));                                     // Tag buffer
    }

    void FetchTransaction::append_to_response(Response& response) const{
        response.append(host_to_network_long(producer_id));
        response.append(host_to_network_long(first_offset));
        response.append(tagged_fields);
    }

    void FetchPartition::append_to_response(Response& response) const{
        response.append(host_to_network_long(partition_index));
        response.append(host_to_network_short(to_underlying(err_code)));
        response.append(high_watermark);
        response.append(last_stable_offset);
        response.append(log_start_offset);
        unsigned_varint_t trans_count = static_cast<uint>(aborted_transactions.size() + 1);
        for (const ubyte& len_portion: trans_count.encode()){
            response.append(len_portion);
        }
        for (const auto& aborted_trans: aborted_transactions){
            aborted_trans.append_to_response(response);
        }
        response.append(host_to_network_long(preferred_read_replica));
        unsigned_varint_t batch_data_size = static_cast<uint>(batches_as_bytes.size() + 1);
        response.append(batch_data_size.encode());
        response.append(batches_as_bytes);
        response.append(static_cast<ubyte>(0));  // Tag buffer
    }

    void FetchResponsePortion::append_to_response(Response& response) const{
        cerr << "Topic UUID: ";
        for (const auto& p: topic_uuid){
            cerr << std::hex << static_cast<uint>(p) << " ";
            response.append(static_cast<ubyte>(p));
        }
        cerr << std::dec << "\n";
        unsigned_varint_t part_count = static_cast<uint>(partitions.size() + 1);
        for (const ubyte& len_portion: part_count.encode()){
            response.append(len_portion);
        }
        for (const auto& partition: partitions){
            partition.append_to_response(response);
        }
        response.append(static_cast<ubyte>(0)); // Tag buffer
    }

    void ProduceRecordErr::append_to_response(Response& response) const {
        response.append(host_to_network_long(batch_index));

        unsigned_varint_t string_length = static_cast<uint>(err_message.size() + 1);
        response.append(string_length.encode());

        if (!err_message.empty()) {
            const ubyte* data_ptr = reinterpret_cast<const ubyte*>(err_message.c_str());
            vector<ubyte> raw_errmsg_bytes{data_ptr, data_ptr + err_message.size()};
            response.append(raw_errmsg_bytes);
        }

        response.append(static_cast<ubyte>(0));         // Tag buffer
    }

    void ProducePartition::append_to_response(Response& response) const {
        response.append(host_to_network_long(partition_index));
        response.append(host_to_network_short(to_underlying(err_code)));
        response.append(base_offset);
        response.append(log_append_time);
        response.append(log_start_offset);

        unsigned_varint_t err_len = static_cast<uint>(errors.size() + 1);
        response.append(err_len.encode());

        for (const auto& batch_err: errors) {
            batch_err.append_to_response(response);
        }

        unsigned_varint_t string_length = static_cast<uint>(err_message.size() + 1);
        response.append(string_length.encode());

        if (!err_message.empty()) {
            const ubyte* data_ptr = reinterpret_cast<const ubyte*>(err_message.c_str());
            vector<ubyte> raw_errmsg_bytes{data_ptr, data_ptr + err_message.size()};
            response.append(raw_errmsg_bytes);
        }

        response.append(static_cast<ubyte>(0));         // Tag buffer
    }

    void ProduceTopic::append_to_response(Response& response) const {
        unsigned_varint_t name_length = static_cast<uint>(topic_name.size() + 1);
        response.append(name_length.encode());

        if (!topic_name.empty()) {
            const ubyte* data_ptr = reinterpret_cast<const ubyte*>(topic_name.c_str());
            vector<ubyte> raw_topic_name_bytes{data_ptr, data_ptr + topic_name.size()};
            response.append(raw_topic_name_bytes);
        }

        unsigned_varint_t partitions_length = static_cast<uint>(partitions.size() + 1);
        response.append(partitions_length.encode());

        for (const auto& partition: partitions) {
            partition.append_to_response(response);
        }

        response.append(static_cast<ubyte>(0));     // Tag buffer
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


    vector<Topic> retrieve_data(const vector<DescribeTopicReqArrEntry>& requested_topics){
        vector<Topic> ret;
        ret.reserve(requested_topics.size());

        vector<Record> topic_records, part_records;

        for (const auto& batch: metadata){
            for (const auto& record: batch.records){
                if (record.is_topic()){
                    topic_records.push_back(record);
                }
                else if (record.is_partition()){
                    part_records.push_back(record);
                }
            }
        }

        auto start_of_tr_list = topic_records.begin();
        auto end_of_tr_list = topic_records.end();

        for (const auto& req_topic: requested_topics){
            auto found_iter = find_if(
                start_of_tr_list,
                end_of_tr_list,
                [req_topic](Record& topic_record){
                    auto& value = topic_record.value;
                    auto& topic_payload = std::get<TopicPayload>(value.payload);
                    return topic_payload.name == req_topic.data;
                }
            );
            if (found_iter == end_of_tr_list){
                Topic not_found;
                not_found.topic_name = req_topic.data;
                not_found.err_code = KafkaErrorCode::UNKNOWN_TOPIC_OR_PARTITION;
                not_found.uuid.fill(0);
                ret.push_back(not_found);
                continue;
            }
            Topic entry;
            entry.err_code = KafkaErrorCode::NO_ERROR;
            entry.topic_name = req_topic.data;
            entry.uuid = std::get<TopicPayload>(found_iter->value.payload).uuid;
            entry.allowed_ops_flags = TopicOperationFlags::ALL;
            fint part_index = 0;
            for (const auto& part_rec: part_records){
                auto& part_pl = std::get<PartitionPayload>(part_rec.value.payload);
                if (part_pl.topic_uuid != entry.uuid){
                    continue;
                }

                auto& last_part = entry.partitions.emplace_back();
                last_part.err_code = KafkaErrorCode::NO_ERROR;
                last_part.partition_index = part_index;
                part_index++;

                last_part.replica_nodes = part_pl.replica_nodes;
                last_part.isr_nodes = part_pl.isr_nodes;
                last_part.elr_nodes = {};
                last_part.last_known_elr_nodes = {};
                last_part.offline_replica_nodes = {};
            }
            ret.push_back(entry);
        }

        return ret;
    }

    void handle_api_versions_request(const Request& request, Response& response){
        fshort version = request.get_api_version();
        if (version >= 0 && version <= 4){
            vector<APIVersionArrEntry> version_entries;
            version_entries.reserve(4);
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
            version_entries.push_back(
                {
                    KafkaAPIKey::FETCH,
                    0,
                    16
                }
            );
            version_entries.push_back(
                {
                    KafkaAPIKey::PRODUCE,
                    0,
                    11
                }
            );

            response.append(host_to_network_short(to_underlying(KafkaErrorCode::NO_ERROR)));    // Error code
            varint_t version_entry_size = static_cast<fint>(version_entries.size() + 1);        // Length of API version entries + 1
            for (const ubyte& len_portion: version_entry_size.encode()){
                response.append(len_portion);
            }

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

        ssize_t moved_offset = starting_point;
        // Since this is an array size, we need to deduce 1 from it.
        auto req_topic_arr_len = unsigned_varint_t::decode_and_advance(buffer, moved_offset) - 1;
        requested_topics.resize(static_cast<ssize_t>(req_topic_arr_len));  // Create empty elements from the start so it is easier to edit them.

        ssize_t offset_for_next_topic = starting_point + 1;
        for (fbyte i = 0; i < req_topic_arr_len; ++i){
            auto string_name_length = unsigned_varint_t::decode_and_advance(buffer, offset_for_next_topic) - 1;
            auto& req_topic_obj = requested_topics.at(i);
            req_topic_obj.data.insert(
                req_topic_obj.data.end(),
                buffer + offset_for_next_topic,
                buffer + offset_for_next_topic + static_cast<ssize_t>(string_name_length)
            );
            // Account for the tag buffer (one empty byte).
            offset_for_next_topic += static_cast<ssize_t>(string_name_length) + 1;
        }

        auto max_part_count_pos = offset_for_next_topic;

        auto max_part_count = read_big_endian<fint>(buffer + max_part_count_pos);
        auto cursor = read_big_endian<fbyte>(buffer + max_part_count_pos + sizeof(fint));


        fshort version = request.get_api_version();
        response.append(static_cast<ubyte>(0)); // Tag buffer (response header v1)
        if (version > 0){
            response.append(host_to_network_short(to_underlying(KafkaErrorCode::UNSUPPORTED_VERSION))); // Error code
        }
        else{
            cerr << "Retrieving data\n";
            vector<Topic> topic_entries = retrieve_data(requested_topics);

            response.append(static_cast<fint>(0));                              // Throttle time
            varint_t topic_count = static_cast<fint>(topic_entries.size() + 1); // Topic array size + 1 (because varint)
            for (const ubyte& len_portion: topic_count.encode()){
                response.append(len_portion);
            }
            sort(
                topic_entries.begin(),
                topic_entries.end(),
                [](Topic& entry1, Topic& entry2){
                    return entry1.topic_name < entry2.topic_name;
                }
            );
            for (const auto& topic: topic_entries){
                topic.append_to_response(response);                             // Append results for each topic to the response.
            }
            response.append(static_cast<ubyte>(0xFF));                          // Next cursor (null for now)
            response.append(static_cast<ubyte>(0));                             // Tag buffer
        }
    }

    void handle_fetch_request(const Request& request, Response& response, char* buffer){
        auto cli_id_size = read_big_endian<fshort>(buffer + 12);

        // Calculation details:
        // 12 bytes from the previous header fields + 2 bytes from the string length + 1 byte for the empty tag buffer = 15 bytes
        // Then we add the size of the string itself, and this determines where to start parsing the body in the message.
        uint starting_point = 15 + cli_id_size;
        ssize_t offset = starting_point;

        // Endian isn't important for now, we'll fix it in later stages if needed.
        // Besides, none of those fields are actually used for the time being.
        // We simply store them so we can advance in the buffer nonetheless.
        auto max_wait_ms = read_and_advance<fint>(buffer, offset);
        auto min_bytes = read_and_advance<fint>(buffer, offset);
        auto max_bytes = read_and_advance<fint>(buffer, offset);
        auto isolation_lv = read_and_advance<fbyte>(buffer, offset);

        // This one IS used though.
        auto session_id = read_and_advance<fint>(buffer, offset);
        // ...except not this one.
        auto session_epoch = read_and_advance<fint>(buffer, offset);

        vector<UUID> requested_uuids;
        auto req_uuid_size = unsigned_varint_t::decode_and_advance(buffer, offset) - 1;
        requested_uuids.resize(static_cast<uint>(req_uuid_size));

        unordered_map<string, vector<fint>> requested_partitions_by_topic;

        for (uint i = 0; i < req_uuid_size; ++i){
            for (ubyte k = 0; k < 16; ++k){
                requested_uuids[i][k] = read_and_advance<ubyte>(buffer, offset);
            }
            string uuid_as_str = uuid_as_string(requested_uuids[i]);
            requested_partitions_by_topic.insert({uuid_as_str, {}});
            auto& part_list = requested_partitions_by_topic.at(uuid_as_str);
            auto partition_count = unsigned_varint_t::decode_and_advance(buffer, offset) - 1;
            part_list.reserve(static_cast<uint>(partition_count));
            for (uint part_index = 0; part_index < partition_count; ++part_index){
                part_list.push_back(read_and_advance<fint>(buffer, offset));    // Partition index: used to check which one is requested
                // These variables will end up unused for now, but it might come in handy later.
                auto current_leader_epoch = read_and_advance<fint>(buffer, offset);
                auto fetch_offset = read_and_advance<flong>(buffer, offset);
                auto last_fetched_epoch = read_and_advance<fint>(buffer, offset);
                auto log_start_offset = read_and_advance<flong>(buffer, offset);
                auto partition_max_bytes = read_and_advance<fint>(buffer, offset);
                auto tagged_field_count = unsigned_varint_t::decode_and_advance(buffer, offset);
            }
        }

        response.append(static_cast<ubyte>(0)); // Tag buffer (response header v1)

        fshort version = request.get_api_version();
        if (version > 16){
            response.append(host_to_network_short(to_underlying(KafkaErrorCode::UNSUPPORTED_VERSION))); // Error code
            return;
        }

        // Metadata was already loaded so we don't need to do it again

        // Setup responses
        vector<FetchResponsePortion> response_portions;
        response_portions.resize(requested_uuids.size());

        for (ubyte i = 0; i < requested_uuids.size(); ++i){
            auto& portion = response_portions.at(i);
            auto& uuid = requested_uuids.at(i);
            portion.topic_uuid = uuid;
            if (topic_exists_as_uuid(uuid)){
                // Find the corresponding topic name
                auto topic_name = get_topic_name_from_uuid(uuid);
                string uuid_as_str = uuid_as_string(uuid);
                auto& requested_indexes = requested_partitions_by_topic.at(uuid_as_str);
                for (const auto& i: requested_indexes){
                    portion.partitions.push_back(
                        {
                            0,                                  // Partition index
                            KafkaErrorCode::NO_ERROR,           // Error code
                            0,                                  // High watermark
                            0,                                  // Last stable offset
                            0,                                  // Log start offset
                            {},                                 // Aborted transactions
                            0,                                  // Preferred read replica
                            {},                                 // Record list
                            {}                                  // Raw batch data
                        }
                    );
                    // Retrieve the partition we just created
                    auto& new_partition = portion.partitions.back();
                    // Retrieve the batches and raw data all at once.
                    new_partition.record_batches = get_record_batches_from_topic(
                        topic_name,
                        i,
                        &new_partition.batches_as_bytes
                    );
                }
            }
            else{
                // Fallback for unknown topics
                portion.partitions.push_back(
                    {
                        0,                                  // Partition index
                        KafkaErrorCode::UNKNOWN_TOPIC_ID,   // Error code
                        0,                                  // High watermark
                        0,                                  // Last stable offset
                        0,                                  // Log start offset
                        {},                                 // Aborted transactions
                        0,                                  // Preferred read replica
                        {},                                 // Record list
                        {}                                  // Raw batch data
                    }
                );
            }
        }

        // Creating the response body
        response.append(static_cast<fint>(0));                                                      // Throttle time (ms)
        response.append(host_to_network_short(to_underlying(KafkaErrorCode::NO_ERROR)));            // Error code
        response.append(static_cast<fint>(0));                                                      // Session ID

        // Add the topic responses one by one
        auto size_as_varint = unsigned_varint_t(static_cast<uint>(response_portions.size() + 1));   // Encoded as a varint so we add 1
        response.append(size_as_varint.encode());
        for (const auto& portion: response_portions){
            portion.append_to_response(response);
        }
        response.append(static_cast<ubyte>(0));                                                     // Tag buffer (response portion array)
    }

    void handle_produce_request(const Request& request, Response& response, char* buffer) {
        auto cli_id_size = read_big_endian<fshort>(buffer + 12);

        // Calculation details:
        // 12 bytes from the previous header fields + 2 bytes from the string length + 1 byte for the empty tag buffer = 15 bytes
        // Then we add the size of the string itself, and this determines where to start parsing the body in the message.
        uint starting_point = 15 + cli_id_size;
        ssize_t offset = starting_point;

        unsigned_varint_t transact_id_size = unsigned_varint_t::decode_and_advance(buffer, offset);
        string transact_id = "";
        if (transact_id_size > 0) {
            transact_id = string(buffer + offset, static_cast<uint>(transact_id_size - 1));
        }

        offset += static_cast<uint>(transact_id_size);

        fshort required_acks = read_and_advance<fshort>(buffer, offset);
        fint timeout = read_be_and_advance<fint>(buffer, offset);

        unsigned_varint_t topic_arr_count = unsigned_varint_t::decode_and_advance(buffer, offset) - 1;
        vector<ProduceReqTopic> requested_topics;
        vector<vector<ubyte>> batches;
        auto tac_as_uint = static_cast<uint>(topic_arr_count);
        requested_topics.reserve(tac_as_uint);

        for (uint i = 0; i < tac_as_uint; ++i) {
            auto& created_topic = requested_topics.emplace_back();
            auto name_length = unsigned_varint_t::decode_and_advance(buffer, offset) - 1;
            uint nl_as_uint = static_cast<uint>(name_length);
            created_topic.topic_name = {buffer + offset, nl_as_uint};
            offset += nl_as_uint;

            auto partition_array_count = unsigned_varint_t::decode_and_advance(buffer, offset) - 1;
            auto pac_as_uint = static_cast<uint>(partition_array_count);
            created_topic.partition_indexes.reserve(pac_as_uint);
            for (uint part_idx = 0; part_idx < pac_as_uint; ++part_idx) {
                auto retrieved_partidx = read_be_and_advance<fint>(buffer, offset);
                created_topic.partition_indexes.push_back(retrieved_partidx);

                auto rec_batch_size = unsigned_varint_t::decode_and_advance(buffer, offset) - 1;
                auto& current_batch = batches.emplace_back();
                cerr << "Reading batch data\n";
                current_batch.insert(
                    current_batch.end(),
                    reinterpret_cast<ubyte*>(buffer + offset),
                    reinterpret_cast<ubyte*>(
                        buffer + offset + static_cast<uint>(rec_batch_size)
                    )
                );
                offset += static_cast<uint>(rec_batch_size);
                auto part_tagged_fields = unsigned_varint_t::decode_and_advance(buffer, offset);    // Ignore for now
            }
            auto topic_tagged_fields = unsigned_varint_t::decode_and_advance(buffer, offset);       // Ignore for now
        }

        // Compute response data
        vector<ProduceTopic> ret_topics;
        ret_topics.resize(tac_as_uint);

        bool topic_exists;

        for (uint i = 0; i < tac_as_uint; ++i) {
            auto& ret_topic = ret_topics[i];
            auto& req_topic = requested_topics[i];

            topic_exists = topic_exists_as_name(req_topic.topic_name);

            ret_topic.topic_name = req_topic.topic_name;
            ret_topic.partitions.reserve(req_topic.partition_indexes.size());
            for (const auto& part_idx: req_topic.partition_indexes) {
                auto& created_ret_part = ret_topic.partitions.emplace_back();
                created_ret_part.partition_index = part_idx;
                created_ret_part.log_append_time = -1;
                if (!topic_exists || !partition_exists_for_topic(ret_topic.topic_name, part_idx)) {
                    created_ret_part.err_code = KafkaErrorCode::UNKNOWN_TOPIC_OR_PARTITION;
                    created_ret_part.base_offset = -1;
                    created_ret_part.log_start_offset = -1;
                }
                else {
                    auto new_offsets = append_batch_to_log_file(req_topic.topic_name, part_idx, batches[i]);
                    created_ret_part.err_code = KafkaErrorCode::NO_ERROR;
                    created_ret_part.base_offset = new_offsets.first;
                    created_ret_part.log_start_offset = new_offsets.second;
                }
            }
        }

        // Append data to response
        response.append(static_cast<ubyte>(0)); // Tag buffer (response header v1)

        unsigned_varint_t topic_arr_size = static_cast<uint>(ret_topics.size() + 1);
        response.append(topic_arr_size.encode());

        for (const auto& ret_topic: ret_topics) {
            ret_topic.append_to_response(response);
        }

        response.append(static_cast<fint>(0));      // Throttle time
        response.append(static_cast<ubyte>(0));     // Tag buffer
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

        // Extract the request API version from the buffer.
        request.set_api_version(read_big_endian<fshort>(buffer + 6));

        // Extract correlation ID from the buffer.
        response.set_correlation_id(read_big_endian<fint>(buffer + 8));
        request.set_correlation_id(response.get_correlation_id());

        switch (static_cast<KafkaAPIKey>(request.get_api_key())){
            case KafkaAPIKey::API_VERSIONS:
                handle_api_versions_request(request, response);
                break;
            case KafkaAPIKey::DESCRIBE_TOPIC_PARTITIONS:
                handle_describe_topic_partitions_request(request, response, buffer);
                break;
            case KafkaAPIKey::FETCH:
                handle_fetch_request(request, response, buffer);
                break;
            case KafkaAPIKey::PRODUCE:
                handle_produce_request(request, response, buffer);
                break;
            default:
                break;
        }

        return 0;
    }
}
