//
// Created by fortwoone on 17/09/2025.
//

#include "cluster_metadata.hpp"


namespace cpp_kafka{
    vector<RecordBatch> load_cluster_metadata(){
        vector<RecordBatch> ret;

        ifstream file(METADATA_FILE_PATH);

        uint record_count;

        while (!file.eof()){
            auto& last_batch = ret.emplace_back();
            file >> last_batch.base_offset;
            file >> last_batch.batch_length;
            file >> last_batch.partition_leader_epoch;
            file >> last_batch.magic;
            file >> last_batch.crc_checksum;
            file >> last_batch.attributes;
            file >> last_batch.last_offset_delta;
            file >> last_batch.base_timestamp;
            file >> last_batch.max_timestamp;
            file >> last_batch.producer_id;
            file >> last_batch.producer_epoch;
            file >> last_batch.base_sequence;

            // Extract records.
            file >> record_count;
            last_batch.records.resize(record_count);
            for (size_t rec_idx = 0; rec_idx < last_batch.records.size(); ++rec_idx){
                auto& rec_ref = last_batch.records.at(rec_idx);
                file >> rec_ref.length;
                file >> rec_ref.attributes;
                file >> rec_ref.timestamp_delta;
                file >> rec_ref.offset_delta;
                file >> rec_ref.key_length;
                rec_ref.key.resize(rec_ref.key_length);
                for (size_t key_idx = 0; key_idx < rec_ref.key.size(); ++key_idx){
                    file >> rec_ref.key.at(key_idx);
                }
                file >> rec_ref.value_length;
                // Parse the payload header.
                auto& rec_header = rec_ref.header;
                file >> rec_header.frame_ver;
                file >> rec_header.type;
                file >> rec_header.version;

                switch (rec_header.type){
                    case 0x0C: // Feature level record
                    {
                        auto& fl_payload = std::get<FeatureLevelPayload>(rec_ref.payload);
                        ubyte name_length;
                        file >> name_length;
                        name_length--; // Encoded as varint, i.e. we need to subtract 1.

                        fl_payload.name.resize(name_length);
                        for (ubyte i = 0; i < name_length; ++i){
                            file >> fl_payload.name.at(i);
                        }
                        file >> fl_payload.feature_level;
                        ubyte tagged_count;
                        file >> tagged_count;  // Extract this so we can skip it and get to the next record.
                        break;
                    }
                    case 0x02: // Topic record
                    {
                        auto& tr_payload = std::get<TopicPayload>(rec_ref.payload);

                        ubyte name_length;
                        file >> name_length;
                        name_length--;
                        tr_payload.name.resize(name_length);
                        for (ubyte i = 0; i < name_length; ++i){
                            file >> tr_payload.name.at(i);
                        }

                        // Extract topic's UUID.
                        for (ubyte k = 0; k < 16; ++k){
                            if (k == 5){
                                tr_payload.uuid[k] = tr_payload.uuid[k - 1];
                                continue;
                            }
                            file >> tr_payload.uuid[k];
                        }

                        ubyte tagged_fields_count;
                        file >> tagged_fields_count;
                        break;
                    }
                    case 0x03:  // Partition record
                    {
                        auto& part_payload = std::get<PartitionPayload>(rec_ref.payload);
                        file >> part_payload.partition_id;
                        for (ubyte k = 0; k < 16; ++k){
                            if (k == 5){
                                part_payload.topic_uuid[k] = part_payload.topic_uuid[k - 1];
                                continue;
                            }
                            file >> part_payload.topic_uuid[k];
                        }
                        ubyte repl_arr_size;
                        file >> repl_arr_size;
                        repl_arr_size--;
                        part_payload.replica_nodes.resize(repl_arr_size);
                        for (ubyte i = 0; i < repl_arr_size; ++i){
                            file >> part_payload.replica_nodes.at(i);
                        }

                        ubyte isr_arr_size;
                        file >> isr_arr_size;
                        isr_arr_size--;
                        part_payload.isr_nodes.resize(isr_arr_size);
                        for (ubyte i = 0; i < isr_arr_size; ++i){
                            file >> part_payload.isr_nodes.at(i);
                        }

                        ubyte rem_arr_size;
                        file >> rem_arr_size;
                        rem_arr_size--;
                        part_payload.rem_replicas.resize(rem_arr_size);
                        for (ubyte i = 0; i < rem_arr_size; ++i){
                            file >> part_payload.rem_replicas.at(i);
                        }

                        ubyte add_arr_size;
                        file >> add_arr_size;
                        add_arr_size--;
                        part_payload.add_replicas.resize(add_arr_size);
                        for (ubyte i = 0; i < add_arr_size; ++i){
                            file >> part_payload.add_replicas.at(i);
                        }

                        file >> part_payload.leader_id;
                        file >> part_payload.leader_epoch;
                        file >> part_payload.part_epoch;

                        ubyte dir_arr_size;
                        file >> dir_arr_size;
                        dir_arr_size--;
                        part_payload.directory_uuids.resize(add_arr_size);
                        for (auto& itm: part_payload.directory_uuids){
                            for (ubyte k = 0; k < 16; ++k){
                                if (k == 5){
                                    itm[k] = itm[k - 1];
                                    continue;
                                }
                                file >> itm[k];
                            }
                        }
                        ubyte tagged_count;
                        file >> tagged_count;
                        break;
                    }
                }
                fbyte header_count;
                file >> header_count;
            }
        }
        file.close();

        return ret;
    }
}
