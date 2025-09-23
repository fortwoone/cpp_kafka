//
// Created by fortwoone on 17/09/2025.
//

#include "cluster_metadata.hpp"


using std::cerr;

namespace cpp_kafka{
    // Internal storage
    static unordered_map<string, UUID> topic_to_uuid;                           // Map topic name to UUID
    static unordered_set<string> topic_uuids;                                   // Existing topics' UUIDs
    static unordered_map<string, vector<PartitionPayload>> uuid_to_payloads;    // Partitions for topic UUIDs

    vector<RecordBatch> get_record_batches_from_topic(const string& topic_name, const fint& partition, vector<ubyte>* raw_byte_arr){
        vector<RecordBatch> ret;

        string file_path = "/tmp/kraft-combined-logs/" + topic_name + "-" + to_string(partition) + "/00000000000000000000.log";

        bool is_metadata = (topic_name == "__cluster_metadata");

        int fd = open(file_path.c_str(), O_RDONLY);
        if (fd < 0){
            throw runtime_error("Failed to open the cluster metadata file.");
        }

        char buf[1024];
        ssize_t bytes_read = read(fd, buf, 1024);

        uint record_count;
        ssize_t offset = 0;

        if (raw_byte_arr != nullptr){
            raw_byte_array->insert(
                raw_byte_arr->end(),
                buf,
                buf + bytes_read
            );
        }

        while (offset < bytes_read){
            // Generate a new batch, and repeat this process until reaching EOF.
            // emplace_back returns a reference to the element created starting from C++14,
            // so we can spare ourselves calling ret.back() to access it.
            auto& last_batch = ret.emplace_back();
            last_batch.base_offset = read_be_and_advance<flong>(buf, offset);
            last_batch.batch_length = read_be_and_advance<fint>(buf, offset);
            last_batch.partition_leader_epoch = read_be_and_advance<uint>(buf, offset);
            last_batch.magic = read_be_and_advance<ubyte>(buf, offset);
            last_batch.crc_checksum = read_be_and_advance<fint>(buf, offset);
            last_batch.attributes = read_be_and_advance<fshort>(buf, offset);
            last_batch.last_offset_delta = read_be_and_advance<fint>(buf, offset);
            last_batch.base_timestamp = read_be_and_advance<flong>(buf, offset);
            last_batch.max_timestamp = read_be_and_advance<flong>(buf, offset);
            last_batch.producer_id = read_be_and_advance<flong>(buf, offset);
            last_batch.producer_epoch = read_be_and_advance<fshort>(buf, offset);
            last_batch.base_sequence = read_be_and_advance<fint>(buf, offset);

            // Extract records.
            record_count = read_be_and_advance<uint>(buf, offset);
            last_batch.records.resize(record_count);
            for (auto& rec_ref : last_batch.records){
                rec_ref.length = varint_t::decode_and_advance(buf, offset);
                rec_ref.attributes = read_be_and_advance<ubyte>(buf, offset);
                rec_ref.timestamp_delta = varint_t::decode_and_advance(buf, offset);
                rec_ref.offset_delta = varint_t::decode_and_advance(buf, offset);

                // Key string operations.
                // First, the key string length is parsed.
                // Beware! Contrary to array sizes, the key string's length is NOT incremented by 1 before being encoded.
                rec_ref.key_length = varint_t::decode_and_advance(buf, offset);
                if (rec_ref.key_length > 0) {
                    // Do not perform this if the key is null or the key length is equal to 0.
                    // The key is considered null if the decoded key length is equal to -1.
                    rec_ref.key.resize(static_cast<uint>(rec_ref.key_length));
                    for (char& key_idx: rec_ref.key) {
                        key_idx = read_and_advance<char>(buf, offset);
                    }
                }
                rec_ref.value_length = varint_t::decode_and_advance(buf, offset);

                // Parse the payload header.
                auto& rec_value = rec_ref.value;
                auto& rec_header = rec_value.header;
                rec_header.frame_ver = read_and_advance<fbyte>(buf, offset);
                rec_header.type = read_be_and_advance<fbyte>(buf, offset);
                rec_header.version = read_be_and_advance<fbyte>(buf, offset);

                // Check if this is the cluster metadata file or a regular partition file.
                if (is_metadata){
                    // This is the metadata file.

                    // Act based on the type read.
                    switch (rec_header.type){
                        case 0x0C: // Feature level record
                        {
                            auto& fl_payload = rec_value.payload.emplace<FeatureLevelPayload>();
                            auto name_length = unsigned_varint_t::decode_and_advance(buf, offset) - 1; // Encoded as varint, i.e. we need to subtract 1.

                            fl_payload.name.resize(static_cast<uint>(name_length));
                            for (ubyte i = 0; i < name_length; ++i){
                                fl_payload.name.at(i) = read_and_advance<char>(buf, offset);
                            }
                            fl_payload.feature_level = read_be_and_advance<fshort>(buf, offset);
                            auto tagged_count = unsigned_varint_t::decode_and_advance(buf, offset);  // Extract this so we can skip it and get to the next record.
                            break;
                        }
                        case 0x02: // Topic record
                        {
                            // We need to replace the held value, as it is a FeatureLevelPayload by default.
                            auto& tr_payload = rec_value.payload.emplace<TopicPayload>();

                            auto name_length = unsigned_varint_t::decode_and_advance(buf, offset) - 1; // Encoded as varint, i.e. we need to subtract 1.
                            tr_payload.name.resize(static_cast<uint>(name_length));

                            for (ubyte i = 0; i < name_length; ++i){
                                tr_payload.name.at(i) = read_and_advance<char>(buf, offset);
                            }

                            offset++;  // Jump one byte ahead to avoid reading incorrect UUIDs.

                            // Extract topic's UUID.
                            for (ubyte k = 0; k < 16; ++k){
                                if (k == 5){
                                    tr_payload.uuid[k] = tr_payload.uuid[k - 1];
                                    continue;
                                }
                                tr_payload.uuid[k] = read_and_advance<ubyte>(buf, offset);
                            }

                            auto uuid_as_str = string(reinterpret_cast<const char*>(tr_payload.uuid.data()), 16);

                            // Reference the topic and its UUID
                            topic_to_uuid.insert(
                                    {tr_payload.name, tr_payload.uuid}
                            );

                            topic_uuids.insert(uuid_as_str);

                            // Insert a partition list.
                            uuid_to_payloads.insert(
                                    {uuid_as_str, {}}
                            );

                            auto tagged_fields_count = unsigned_varint_t::decode_and_advance(buf, offset);
                            break;
                        }
                        case 0x03:  // Partition record
                        {
                            // We need to replace the held value, as it is a FeatureLevelPayload by default.
                            auto& part_payload = rec_value.payload.emplace<PartitionPayload>();

                            part_payload.partition_id = read_be_and_advance<fint>(buf, offset);
                            offset++;  // Jump one byte ahead before reading to avoid reading incorrect values.
                            for (ubyte k = 0; k < 16; ++k){
                                if (k == 5){
                                    part_payload.topic_uuid[k] = part_payload.topic_uuid[k - 1];
                                    continue;
                                }
                                part_payload.topic_uuid[k] = read_and_advance<ubyte>(buf, offset);
                            }

                            // Encoded as a varint, so we need to deduce 1 from this value.
                            auto repl_arr_size = unsigned_varint_t::decode_and_advance(buf, offset) - 1;
                            part_payload.replica_nodes.resize(static_cast<uint>(repl_arr_size));
                            for (ubyte i = 0; i < repl_arr_size; ++i){
                                part_payload.replica_nodes.at(i) = read_be_and_advance<fint>(buf, offset);
                            }

                            // Encoded as a varint, so we need to deduce 1 from this value.
                            auto isr_arr_size = unsigned_varint_t::decode_and_advance(buf, offset) - 1;
                            part_payload.isr_nodes.resize(static_cast<uint>(isr_arr_size));
                            for (ubyte i = 0; i < isr_arr_size; ++i){
                                part_payload.isr_nodes.at(i) = read_be_and_advance<fint>(buf, offset);
                            }

                            // Encoded as a varint, so we need to deduce 1 from this value.
                            auto rem_arr_size = unsigned_varint_t::decode_and_advance(buf, offset) - 1;
                            part_payload.rem_replicas.resize(static_cast<uint>(rem_arr_size));
                            for (ubyte i = 0; i < rem_arr_size; ++i){
                                part_payload.rem_replicas.at(i) = read_be_and_advance<fint>(buf, offset);
                            }

                            // Encoded as a varint, so we need to deduce 1 from this value.
                            auto add_arr_size = unsigned_varint_t::decode_and_advance(buf, offset) - 1;
                            part_payload.add_replicas.resize(static_cast<uint>(add_arr_size));
                            for (ubyte i = 0; i < add_arr_size; ++i){
                                part_payload.add_replicas.at(i) = read_be_and_advance<fint>(buf, offset);
                            }

                            part_payload.leader_id = read_be_and_advance<uint>(buf, offset);
                            part_payload.leader_epoch = read_be_and_advance<uint>(buf, offset);
                            part_payload.part_epoch = read_be_and_advance<uint>(buf, offset);

                            auto dir_arr_size = unsigned_varint_t::decode_and_advance(buf, offset) - 1;
                            part_payload.directory_uuids.resize(static_cast<uint>(dir_arr_size));
                            for (auto& itm: part_payload.directory_uuids){
                                offset++;  // Jump one byte ahead to avoid reading incorrect values.
                                for (ubyte k = 0; k < 16; ++k){
                                    if (k == 5){
                                        itm[k] = itm[k - 1];
                                        continue;
                                    }
                                    itm[k] = read_and_advance<ubyte>(buf, offset);
                                }
                            }

                            string uuid_as_str = {
                                    reinterpret_cast<const char*>(part_payload.topic_uuid.data()),
                                    16
                            };
                            uuid_to_payloads[uuid_as_str].push_back(part_payload);
                            auto tagged_count = unsigned_varint_t::decode_and_advance(buf, offset);
                            break;
                        }
                        default:
                            throw runtime_error("Unsupported record type.");
                    }
                }
                else{
                    // Regular record
                    auto& as_vec = rec_value.payload.emplace<vector<ubyte>>();
                    as_vec.resize(static_cast<uint>(rec_ref.value_length - static_cast<fint>(sizeof(PayloadHeader))));

                    for (size_t i = 0; i < as_vec.size(); ++i){
                        as_vec[i] = read_and_advance<ubyte>(buf, offset);
                    }
                }
                auto header_count = unsigned_varint_t::decode_and_advance(buf, offset);
            }
        }
        close(fd);

        return ret;
    }

    vector<RecordBatch> load_cluster_metadata(){
        return get_record_batches_from_topic("__cluster_metadata", 0);
    }

    bool topic_exists_as_uuid(const UUID& uuid){
        string uuid_as_str = {
            reinterpret_cast<const char*>(uuid.data()),
            16
        };
        return topic_uuids.contains(uuid_as_str);
    }

    string get_topic_name_from_uuid(const UUID& uuid){
        for (const auto& [t_name, t_uuid]: topic_to_uuid){
            if (t_uuid == uuid){
                return t_name;
            }
        }
        throw invalid_argument("Given UUID does not refer to a topic.");
    }

    vector<PartitionPayload> get_partitions_for_uuid(const UUID& uuid){
        string uuid_as_str = {
            reinterpret_cast<const char*>(uuid.data()),
            16
        };
        return uuid_to_payloads.at(uuid_as_str);
    }

    static size_t get_partition_count_for_uuid(const UUID& uuid){
        string uuid_as_str = {
            reinterpret_cast<const char*>(uuid.data()),
            16
        };
        return uuid_to_payloads.at(uuid_as_str).size();
    }

    vector<ubyte> get_raw_record_batch(const UUID& uuid, const fint& partition){
        if (!topic_exists_as_uuid(uuid)){
            throw invalid_argument("Given UUID does not refer to a topic.");
        }
        auto part_count = get_partition_count_for_uuid(uuid);
        if (partition >= part_count){
            throw out_of_range("Partition index is out of bounds for the current topic.");
        }

        string topic_name, file_path;

        for (const auto& [name, t_uuid]: topic_to_uuid){
            if (uuid == t_uuid){
                topic_name = name;
                break;
            }
        }

        file_path = "/tmp/kraft-combined-logs/" + topic_name + "-" + to_string(partition) + "/00000000000000000000.log";

        ifstream log_file(file_path, std::ios::binary);
        if (!log_file){
            throw runtime_error("Could not open the log file.");
        }

        return {istreambuf_iterator<char>(log_file), {}};
    }
}
