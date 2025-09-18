//
// Created by fortwoone on 17/09/2025.
//

#include "cluster_metadata.hpp"
#include <iostream>
#include <iomanip>


using std::cerr;

namespace cpp_kafka{
    vector<RecordBatch> load_cluster_metadata(){
        vector<RecordBatch> ret;

//        ifstream file(METADATA_FILE_PATH);
        int fd = open(METADATA_FILE_PATH, O_RDONLY);
        if (fd < 0){
            throw runtime_error("Failed to open the cluster metadata file.");
        }

        char buf[1024];
        ssize_t bytes_read = read(fd, buf, 1024);
        cerr << "Read " << bytes_read << " bytes from cluster metadata file\n";

        uint record_count;
        ssize_t offset = 0;

        while (offset < bytes_read){
            auto& last_batch = ret.emplace_back();
            cerr << "Created a new record batch\n";
            last_batch.base_offset = read_be_and_advance<flong>(buf, offset);
            cerr << "Read base offset: " << last_batch.base_offset << "\n";
            last_batch.batch_length = read_be_and_advance<fint>(buf, offset);
            cerr << "Read batch length: " << last_batch.batch_length << "\n";
            last_batch.partition_leader_epoch = read_be_and_advance<uint>(buf, offset);
            cerr << "Read partition leader epoch: " << last_batch.partition_leader_epoch << "\n";
            last_batch.magic = read_be_and_advance<ubyte>(buf, offset);
            cerr << "Read magic byte\n";
            last_batch.crc_checksum = read_be_and_advance<fint>(buf, offset);
            cerr << "Read CRC checksum: " << std::hex << last_batch.crc_checksum << std::dec << "\n";
            last_batch.attributes = read_be_and_advance<fshort>(buf, offset);
            cerr << "Read batch attributes: " << last_batch.attributes << "\n";
            last_batch.last_offset_delta = read_be_and_advance<fint>(buf, offset);
            cerr << "Read last offset delta: " << last_batch.last_offset_delta << "\n";
            last_batch.base_timestamp = read_be_and_advance<flong>(buf, offset);
            cerr << "Read base timestamp: " << last_batch.base_timestamp << "\n";
            last_batch.max_timestamp = read_be_and_advance<flong>(buf, offset);
            cerr << "Read max timestamp: " << last_batch.max_timestamp << "\n";
            last_batch.producer_id = read_be_and_advance<flong>(buf, offset);
            cerr << "Read producer ID: " << last_batch.producer_id << "\n";
            last_batch.producer_epoch = read_be_and_advance<fshort>(buf, offset);
            cerr << "Read producer epoch: " << last_batch.producer_epoch << "\n";
            last_batch.base_sequence = read_be_and_advance<fint>(buf, offset);
            cerr << "Read base sequence: " << last_batch.base_sequence << "\n";

            // Extract records.
            record_count = read_be_and_advance<uint>(buf, offset);
            cerr << "Read record count: " << std::hex << record_count << std::dec << "\n",
            last_batch.records.resize(record_count);
            cerr << "Resized record vector\n";
            for (auto& rec_ref : last_batch.records){
                rec_ref.length = varint_t::decode_and_advance(buf, offset);
                cerr << "Read record length: " << static_cast<ushort>(rec_ref.length) << "\n";
                rec_ref.attributes = read_be_and_advance<ubyte>(buf, offset);
                cerr << "Read record attributes: " << std::hex << static_cast<ushort>(rec_ref.attributes) << std::dec << "\n";
                rec_ref.timestamp_delta = varint_t::decode_and_advance(buf, offset);
                cerr << "Read timestamp delta: " << std::hex << static_cast<fint>(rec_ref.timestamp_delta) << std::dec << "\n";
                rec_ref.offset_delta = varint_t::decode_and_advance(buf, offset);
                cerr << "Read offset delta: " << static_cast<fint>(rec_ref.offset_delta) << "\n";
                rec_ref.key_length = varint_t::decode_and_advance(buf, offset);
                cerr << "Read key length: " << static_cast<fint>(rec_ref.key_length) << "\n";
                if (rec_ref.key_length > -1) {
                    // Do not perform this if the key is null.
                    rec_ref.key.resize(static_cast<uint>(rec_ref.key_length));
                    cerr << "Resized key string\n";
                    for (char& key_idx: rec_ref.key) {
                        key_idx = read_and_advance<char>(buf, offset);
                    }
                    cerr << "Key string: " << rec_ref.key << "\n";
                }
                rec_ref.value_length = varint_t::decode_and_advance(buf, offset);
                cerr << "Value length: " << static_cast<fint>(rec_ref.value_length) << "\n";
                // Parse the payload header.
                auto& rec_header = rec_ref.header;
                rec_header.frame_ver = read_be_and_advance<fbyte>(buf, offset);
                rec_header.type = read_be_and_advance<fbyte>(buf, offset);
                rec_header.version = read_be_and_advance<fbyte>(buf, offset);

                switch (rec_header.type){
                    case 0x0C: // Feature level record
                    {
                        auto& fl_payload = std::get<FeatureLevelPayload>(rec_ref.payload);
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
                        auto& tr_payload = std::get<TopicPayload>(rec_ref.payload);

                        unsigned_varint_t name_length = unsigned_varint_t::decode_and_advance(buf, offset) - 1; // Encoded as varint, i.e. we need to subtract 1.
                        tr_payload.name.resize(static_cast<uint>(name_length));
                        for (ubyte i = 0; i < name_length; ++i){
                            tr_payload.name.at(i) = read_and_advance<char>(buf, offset);
                        }

                        // Extract topic's UUID.
                        for (ubyte k = 0; k < 16; ++k){
                            if (k == 5){
                                tr_payload.uuid[k] = tr_payload.uuid[k - 1];
                                continue;
                            }
                            tr_payload.uuid[k] = read_and_advance<ubyte>(buf, offset);
                        }

                        auto tagged_fields_count = unsigned_varint_t::decode_and_advance(buf, offset);
                        break;
                    }
                    case 0x03:  // Partition record
                    {
                        auto& part_payload = std::get<PartitionPayload>(rec_ref.payload);
                        part_payload.partition_id = read_be_and_advance<fint>(buf, offset);
                        for (ubyte k = 0; k < 16; ++k){
                            if (k == 5){
                                part_payload.topic_uuid[k] = part_payload.topic_uuid[k - 1];
                                continue;
                            }
                            part_payload.topic_uuid[k] = read_and_advance<ubyte>(buf, offset);
                        }
                        auto repl_arr_size = unsigned_varint_t::decode_and_advance(buf, offset) - 1;
                        part_payload.replica_nodes.resize(static_cast<uint>(repl_arr_size));
                        for (ubyte i = 0; i < repl_arr_size; ++i){
                            part_payload.replica_nodes.at(i) = read_be_and_advance<fint>(buf, offset);
                        }

                        auto isr_arr_size = unsigned_varint_t::decode_and_advance(buf, offset);
                        part_payload.isr_nodes.resize(static_cast<uint>(isr_arr_size));
                        for (ubyte i = 0; i < isr_arr_size; ++i){
                            part_payload.isr_nodes.at(i) = read_be_and_advance<fint>(buf, offset);
                        }

                        auto rem_arr_size = unsigned_varint_t::decode_and_advance(buf, offset) - 1;
                        part_payload.rem_replicas.resize(static_cast<uint>(rem_arr_size));
                        for (ubyte i = 0; i < rem_arr_size; ++i){
                            part_payload.rem_replicas.at(i) = read_be_and_advance<fint>(buf, offset);
                        }

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
                            for (ubyte k = 0; k < 16; ++k){
                                if (k == 5){
                                    itm[k] = itm[k - 1];
                                    continue;
                                }
                                itm[k] = read_and_advance<ubyte>(buf, offset);
                            }
                        }
                        auto tagged_count = unsigned_varint_t::decode_and_advance(buf, offset);
                        break;
                    }
                }
                auto header_count = unsigned_varint_t::decode_and_advance(buf, offset);
            }
        }
        close(fd);
//        file.close();

        return ret;
    }
}
