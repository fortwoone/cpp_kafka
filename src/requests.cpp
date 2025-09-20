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
        unsigned_varint_t rec_count = static_cast<uint>(record_batches.size() + 1);
        for (const ubyte& len_portion: rec_count.encode()){
            response.append(len_portion);
        }
        for (const auto& rec: record_batches){
            append_record_batch_to_response(response, rec);
        }
        response.append(static_cast<ubyte>(0));  // Tag buffer
    }

    void FetchResponsePortion::append_to_response(Response& response) const{
        for (const auto& p: topic_uuid){
            response.append(static_cast<ubyte>(p));
        }
        unsigned_varint_t part_count = static_cast<uint>(partitions.size() + 1);
        for (const ubyte& len_portion: part_count.encode()){
            response.append(len_portion);
        }
        for (const auto& partition: partitions){
            partition.append_to_response(response);
        }
        response.append(static_cast<ubyte>(0)); // Tag buffer
    }

    void append_payload_header_to_response(Response& response, const PayloadHeader& header){
        response.append(convert_to_big_endian(header.frame_ver));           // Frame version
        response.append(convert_to_big_endian(header.type));                // Metadata record type (big-endian)
        response.append(convert_to_big_endian(header.version));             // Metadata record version (big-endian)
    }

    void append_feature_lv_payload_to_response(Response& response, const FeatureLevelPayload& payload){
        auto size_as_uvarint = unsigned_varint_t(static_cast<uint>(payload.name.size() + 1));
        response.append(size_as_uvarint.encode());                      // Name length as unsigned varint
        for (const auto& c: payload.name){
            response.append(static_cast<char>(c));                      // Feature name
        }
        response.append(convert_to_big_endian(payload.feature_level));  // Feature level (big-endian)
        response.append(static_cast<ubyte>(0));                         // Tag buffer
    }

    void append_topic_payload_to_response(Response& response, const TopicPayload& payload){
        auto size_as_uvarint = unsigned_varint_t(static_cast<uint>(payload.name.size() + 1));
        response.append(size_as_uvarint.encode());  // Name length as unsigned varint
        for (const auto& c: payload.name){
            response.append(static_cast<char>(c));  // Topic name
        }
        for (const ubyte& b: payload.uuid){
            response.append(b);                     // Topic UUID
        }
        response.append(static_cast<ubyte>(0));     // Tag buffer
    }

    void append_partition_payload_to_response(Response& response, const PartitionPayload& payload){
        response.append(convert_to_big_endian(payload.partition_id));   // Partition ID (big-endian)
        for (const ubyte& b: payload.topic_uuid){
            response.append(b);                                         // Topic UUID
        }

        auto repl_size_uvar = unsigned_varint_t(static_cast<uint>(payload.replica_nodes.size() + 1));
        response.append(repl_size_uvar.encode());                       // Length of replica nodes array as unsigned varint
        for (const fint& repl_node: payload.replica_nodes){
            response.append(convert_to_big_endian(repl_node));          // Replica nodes (big-endian)
        }

        auto isr_size_uvar = unsigned_varint_t(static_cast<uint>(payload.isr_nodes.size() + 1));
        response.append(isr_size_uvar.encode());                        // Length of ISR nodes array as unsigned varint
        for (const fint& isr_node: payload.isr_nodes){
            response.append(convert_to_big_endian(isr_node));           // ISR nodes (big-endian)
        }

        auto rem_size_uvar = unsigned_varint_t(static_cast<uint>(payload.rem_replicas.size() + 1));
        response.append(rem_size_uvar.encode());                        // Length of removing replica nodes array as unsigned varint
        for (const fint& rem_repl_node: payload.rem_replicas){
            response.append(convert_to_big_endian(rem_repl_node));      // Removing replica nodes (big-endian)
        }

        auto add_size_uvar = unsigned_varint_t(static_cast<uint>(payload.add_replicas.size() + 1));
        response.append(add_size_uvar.encode());
        for (const fint& add_repl_node: payload.add_replicas){
            response.append(convert_to_big_endian(add_repl_node));      // Adding replica nodes (big-endian)
        }

        response.append(convert_to_big_endian(payload.leader_id));      // Leader ID (big-endian)
        response.append(convert_to_big_endian(payload.leader_epoch));   // Leader epoch (big-endian)
        response.append(convert_to_big_endian(payload.part_epoch));     // Partition epoch (big-endian)

        auto uuid_arr_size = unsigned_varint_t(static_cast<uint>(payload.directory_uuids.size() + 1));
        response.append(uuid_arr_size.encode());                        // Size of directory UUID array as unsigned varint
        for (const auto& dir_uuid: payload.directory_uuids){
            for (const ubyte& b: dir_uuid){
                response.append(b);                                     // Directory UUIDs
            }
        }

        response.append(static_cast<ubyte>(0));                         // Tag buffer
    }

    void append_metadata_rec_payload_to_response(Response& response, const MetadataRecordPayload& payload){
        append_payload_header_to_response(response, payload.header);
        if (holds_alternative<FeatureLevelPayload>(payload.payload)){
            return append_feature_lv_payload_to_response(response, std::get<FeatureLevelPayload>(payload.payload));
        }
        if (holds_alternative<TopicPayload>(payload.payload)){
            return append_topic_payload_to_response(response, std::get<TopicPayload>(payload.payload));
        }
        append_partition_payload_to_response(response, std::get<PartitionPayload>(payload.payload));
    }

    void append_record_to_response(Response& response, const Record& record){
        response.append(record.length.encode());                        // Record's length as a varint
        response.append(convert_to_big_endian(record.attributes));      // Record's attributes (big-endian)
        response.append(record.timestamp_delta.encode());               // Record's timestamp delta as a varint
        response.append(record.offset_delta.encode());                  // Record's offset delta as a varint

        response.append(record.key_length.encode());                    // Record's key length as a varint
        if (record.key_length > 0) {
            for (const auto& c: record.key) {
                response.append(c);                                     // Record's key
            }
        }

        response.append(record.value_length.encode());                  // Record's value length as a varint
        if (record.value_length > 0){
            if (holds_alternative<vector<ubyte>>(record.value)){
                response.append(std::get<vector<ubyte>>(record.value)); // Record's raw value. Append regardless of its actual format.
            }
            else{
                append_metadata_rec_payload_to_response(                // Record's payload header
                    response,
                    std::get<MetadataRecordPayload>(record.value)
                );
            }
        }
        response.append(static_cast<ubyte>(0));                         // Headers array count
    }

    void append_record_batch_to_response(Response& response, const RecordBatch& record_batch){
        cerr << "Base offset: " << std::hex << record_batch.base_offset << "\n";
        auto base_off_to_be_vec = convert_to_big_endian(record_batch.base_offset);
        decltype(record_batch.base_offset) base_off_to_be;
        memcpy(&base_off_to_be, base_off_to_be_vec.data(), sizeof(base_off_to_be));
        cerr << "Base offset (big-endian): " << base_off_to_be << "\n";

        cerr << "Batch length: " << std::hex << record_batch.batch_length << "\n";
        auto batch_len_to_be_vec = convert_to_big_endian(record_batch.batch_length);
        decltype(record_batch.batch_length) batch_len_to_be;
        memcpy(&batch_len_to_be, batch_len_to_be_vec.data(), sizeof(batch_len_to_be));
        cerr << "Batch length (big-endian): " << batch_len_to_be << "\n";

        cerr << "Partition leader epoch: " << std::hex << record_batch.partition_leader_epoch << "\n";
        auto part_lead_epoch_be_vec = convert_to_big_endian(record_batch.partition_leader_epoch);
        decltype(record_batch.partition_leader_epoch) part_lead_epoch_be;
        memcpy(&part_lead_epoch_be, part_lead_epoch_be_vec.data(), sizeof(part_lead_epoch_be));
        cerr << "Partition leader epoch (big-endian): " << part_lead_epoch_be << "\n";

        cerr << "CRC checksum: " << std::hex << record_batch.crc_checksum << "\n";
        auto crc_checksum_be_vec = convert_to_big_endian(record_batch.crc_checksum);
        decltype(record_batch.crc_checksum) crc_checksum_be;
        memcpy(&crc_checksum_be, crc_checksum_be_vec.data(), sizeof(crc_checksum_be));
        cerr << "CRC checksum (big-endian): " << crc_checksum_be << "\n";

        response.append(convert_to_big_endian(record_batch.base_offset));                       // Batch's base offset (big-endian)
        response.append(convert_to_big_endian(record_batch.batch_length));                      // Batch's length (big-endian)
        response.append(convert_to_big_endian(record_batch.partition_leader_epoch));            // Batch's partition leader epoch (big-endian)
        response.append(convert_to_big_endian(record_batch.magic));                             // Batch's magic byte (big-endian)
        response.append(convert_to_big_endian(record_batch.crc_checksum));                      // Batch's CRC checksum (big-endian)
        response.append(convert_to_big_endian(record_batch.attributes));                        // Batch's attribute bitfield (big-endian)
        response.append(convert_to_big_endian(record_batch.last_offset_delta));                 // Batch's last offset delta (big-endian)
        response.append(convert_to_big_endian(record_batch.base_timestamp));                    // Batch's base timestamp (big-endian)
        response.append(convert_to_big_endian(record_batch.max_timestamp));                     // Batch's max timestamp (big-endian)
        response.append(convert_to_big_endian(record_batch.producer_id));                       // Batch's producer ID (big-endian)
        response.append(convert_to_big_endian(record_batch.producer_epoch));                    // Batch's producer epoch (big-endian)
        response.append(convert_to_big_endian(record_batch.base_sequence));                     // Batch's base sequence (big-endian)

        response.append(convert_to_big_endian(static_cast<uint>(record_batch.records.size()))); // Length of batch's records array (big-endian)
        for (const auto& rec: record_batch.records){
            append_record_to_response(response, rec);
        }
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
                    auto& value = std::get<MetadataRecordPayload>(topic_record.value);
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
            auto& found_val = std::get<MetadataRecordPayload>(found_iter->value);
            entry.uuid = std::get<TopicPayload>(found_val.payload).uuid;
            entry.allowed_ops_flags = TopicOperationFlags::ALL;
            fint part_index = 0;
            for (const auto& part_rec: part_records){
                auto& rec_val = std::get<MetadataRecordPayload>(part_rec.value);
                auto& part_pl = std::get<PartitionPayload>(rec_val.payload);
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
            version_entries.reserve(3);
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

        vector<TopicUUID> requested_uuids;
        auto req_uuid_size = unsigned_varint_t::decode_and_advance(buffer, offset) - 1;
        requested_uuids.resize(static_cast<uint>(req_uuid_size));

        unordered_map<string, vector<fint>> requested_partitions_by_topic;

        for (uint i = 0; i < req_uuid_size; ++i){
            offset++;  // Jump one byte ahead to avoid reading incorrect values
            for (ubyte k = 0; k < 16; ++k){
                if (k == 5){
                    requested_uuids[i][k] = requested_uuids[i][k - 1];
                    continue;
                }
                requested_uuids[i][k] = read_and_advance<ubyte>(buffer, offset);
            }
            string uuid_as_str = {
                reinterpret_cast<const char*>(requested_uuids[i].data()),
                16
            };
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

        vector<FetchResponsePortion> response_portions;
        response_portions.resize(requested_uuids.size());
        for (ubyte i = 0; i < requested_uuids.size(); ++i){
            auto& portion = response_portions.at(i);
            auto& uuid = requested_uuids.at(i);
            portion.topic_uuid = uuid;
            auto topic_name = get_topic_name_from_uuid(uuid);
            if (topic_exists_as_uuid(uuid)){
                string uuid_as_str = {
                    reinterpret_cast<const char*>(uuid.data()),
                    16
                };
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
                            {}                                  // Record list
                        }
                    );
                    auto& new_partition = portion.partitions.back();
                    new_partition.record_batches = get_record_batches_from_topic(topic_name, i);
                }
            }
            else{
                portion.partitions.push_back(
                    {
                        0,                                  // Partition index
                        KafkaErrorCode::UNKNOWN_TOPIC_ID,   // Error code
                        0,                                  // High watermark
                        0,                                  // Last stable offset
                        0,                                  // Log start offset
                        {},                                 // Aborted transactions
                        0,                                  // Preferred read replica
                        {}                                  // Record list
                    }
                );
            }
        }


        response.append(static_cast<fint>(0));                                                      // Throttle time (ms)
        response.append(host_to_network_short(to_underlying(KafkaErrorCode::NO_ERROR)));            // Error code
        response.append(static_cast<fint>(0));                                                      // Session ID
        auto size_as_varint = unsigned_varint_t(static_cast<uint>(response_portions.size() + 1));   // Encoded as a varint so we add 1
        for (const ubyte& len_portion: size_as_varint.encode()){
            response.append(len_portion);
        }
        for (const auto& portion: response_portions){
            portion.append_to_response(response);
        }
        response.append(static_cast<ubyte>(0));                                                     // Tag buffer (response portion array)
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
            case KafkaAPIKey::FETCH:
                handle_fetch_request(request, response, buffer);
                break;
            default:
                break;
        }

        return 0;
    }
}
