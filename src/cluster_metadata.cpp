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
            cerr << "Read base timestamp\n";
            last_batch.max_timestamp = read_be_and_advance<flong>(buf, offset);
            cerr << "Read max timestamp\n";
            last_batch.producer_id = read_be_and_advance<flong>(buf, offset);
            cerr << "Read producer ID: " << last_batch.producer_id << "\n";
            last_batch.producer_epoch = read_be_and_advance<ushort>(buf, offset);
            cerr << "Read producer epoch\n";
            last_batch.base_sequence = read_be_and_advance<fint>(buf, offset);
            cerr << "Read base sequence\n";

            // Extract records.
            record_count = read_and_advance<uint>(buf, offset);
            cerr << "Read record count: " << record_count << "\n",
            last_batch.records.resize(record_count);
            cerr << "Resized record vector\n";
            for (size_t rec_idx = 0; rec_idx < last_batch.records.size(); ++rec_idx){
                auto& rec_ref = last_batch.records.at(rec_idx);
                rec_ref.length = read_be_and_advance<fbyte>(buf, offset);
                cerr << "Read record length\n";
                rec_ref.attributes = read_be_and_advance<ubyte>(buf, offset);
                cerr << "Read record attributes\n";
                rec_ref.timestamp_delta = read_be_and_advance<fbyte>(buf, offset);
                cerr << "Read timestamp delta\n";
                rec_ref.offset_delta = read_be_and_advance<fbyte>(buf, offset);
                cerr << "Read offset delta\n";
                rec_ref.key_length = read_be_and_advance<fbyte>(buf, offset);
                cerr << "Read key length\n";
                rec_ref.key.resize(rec_ref.key_length);
                cerr << "Resized key string\n";
                for (size_t key_idx = 0; key_idx < rec_ref.key.size(); ++key_idx){
                    rec_ref.key.at(key_idx) = read_and_advance<char>(buf, offset);
                }
                rec_ref.value_length = read_be_and_advance<fbyte>(buf, offset);
                // Parse the payload header.
                auto& rec_header = rec_ref.header;
                rec_header.frame_ver = read_be_and_advance<fbyte>(buf, offset);
                rec_header.type = read_be_and_advance<fbyte>(buf, offset);
                rec_header.version = read_be_and_advance<fbyte>(buf, offset);

                switch (rec_header.type){
                    case 0x0C: // Feature level record
                    {
                        auto& fl_payload = std::get<FeatureLevelPayload>(rec_ref.payload);
                        ubyte name_length = read_be_and_advance<ubyte>(buf, offset) - 1; // Encoded as varint, i.e. we need to subtract 1.

                        fl_payload.name.resize(name_length);
                        for (ubyte i = 0; i < name_length; ++i){
                            fl_payload.name.at(i) = read_and_advance<char>(buf, offset);
                        }
                        fl_payload.feature_level = read_be_and_advance<fshort>(buf, offset);
                        ubyte tagged_count = read_be_and_advance<ubyte>(buf, offset);  // Extract this so we can skip it and get to the next record.
                        break;
                    }
                    case 0x02: // Topic record
                    {
                        auto& tr_payload = std::get<TopicPayload>(rec_ref.payload);

                        ubyte name_length = read_be_and_advance<ubyte>(buf, offset) - 1; // Encoded as varint, i.e. we need to subtract 1.
                        name_length--;
                        tr_payload.name.resize(name_length);
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

                        ubyte tagged_fields_count = read_and_advance<ubyte>(buf, offset);
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
                        ubyte repl_arr_size = read_and_advance<ubyte>(buf, offset) - 1;
                        part_payload.replica_nodes.resize(repl_arr_size);
                        for (ubyte i = 0; i < repl_arr_size; ++i){
                            part_payload.replica_nodes.at(i) = read_be_and_advance<fint>(buf, offset);
                        }

                        ubyte isr_arr_size = read_and_advance<ubyte>(buf, offset);
                        part_payload.isr_nodes.resize(isr_arr_size);
                        for (ubyte i = 0; i < isr_arr_size; ++i){
                            part_payload.isr_nodes.at(i) = read_be_and_advance<fint>(buf, offset);
                        }

                        ubyte rem_arr_size = read_and_advance<ubyte>(buf, offset) - 1;
                        part_payload.rem_replicas.resize(rem_arr_size);
                        for (ubyte i = 0; i < rem_arr_size; ++i){
                            part_payload.rem_replicas.at(i) = read_be_and_advance<fint>(buf, offset);
                        }

                        ubyte add_arr_size = read_and_advance<ubyte>(buf, offset) - 1;
                        part_payload.add_replicas.resize(add_arr_size);
                        for (ubyte i = 0; i < add_arr_size; ++i){
                            part_payload.add_replicas.at(i) = read_be_and_advance<fint>(buf, offset);
                        }

                        part_payload.leader_id = read_be_and_advance<uint>(buf, offset);
                        part_payload.leader_epoch = read_be_and_advance<uint>(buf, offset);
                        part_payload.part_epoch = read_be_and_advance<uint>(buf, offset);

                        ubyte dir_arr_size = read_and_advance<ubyte>(buf, offset) - 1;
                        part_payload.directory_uuids.resize(add_arr_size);
                        for (auto& itm: part_payload.directory_uuids){
                            for (ubyte k = 0; k < 16; ++k){
                                if (k == 5){
                                    itm[k] = itm[k - 1];
                                    continue;
                                }
                                itm[k] = read_and_advance<ubyte>(buf, offset);
                            }
                        }
                        ubyte tagged_count = read_and_advance<ubyte>(buf, offset);
                        break;
                    }
                }
                fbyte header_count = read_and_advance<fbyte>(buf, offset);
            }
        }
        close(fd);
//        file.close();

        return ret;
    }
}
