//
// Created by fortwoone on 17/09/2025.
//

#pragma once

#include <fcntl.h>
#include <fstream>
#include <stdexcept>
#include <string>
#include <variant>
#include <vector>

#include "utils.hpp"

namespace cpp_kafka{
    using std::holds_alternative;
//    using std::ifstream;
    using std::runtime_error;
    using std::string;
    using std::variant;
    using std::vector;

    constexpr char METADATA_FILE_PATH[] = "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log";

    struct PayloadHeader{
        fbyte frame_ver, type, version;
    };

    struct FeatureLevelPayload{
        string name;
        fshort feature_level;
    };

    struct TopicPayload{
        string name;
        TopicUUID uuid;
    };

    struct PartitionPayload{
        fint partition_id;
        TopicUUID topic_uuid;
        vector<fint> replica_nodes, isr_nodes, rem_replicas, add_replicas;
        uint leader_id, leader_epoch;
        uint part_epoch;
        vector<TopicUUID> directory_uuids;
    };

    struct Record{
        fbyte length;
        ubyte attributes;
        fbyte timestamp_delta;
        fbyte offset_delta;
        fbyte key_length;
        string key;
        fbyte value_length;
        PayloadHeader header;
        variant<FeatureLevelPayload, TopicPayload, PartitionPayload> payload;

        [[nodiscard]] bool is_feature_level() const{
            return holds_alternative<FeatureLevelPayload>(payload);
        }

        [[nodiscard]] bool is_topic() const{
            return holds_alternative<TopicPayload>(payload);
        }

        [[nodiscard]] bool is_partition() const{
            return holds_alternative<PartitionPayload>(payload);
        }
    };

    struct RecordBatch{
        flong base_offset;
        fint batch_length;
        uint partition_leader_epoch;
        ubyte magic;
        fint crc_checksum;
        fshort attributes;
        fint last_offset_delta;
        flong base_timestamp, max_timestamp;
        flong producer_id;
        ushort producer_epoch;
        fint base_sequence;
        vector<Record> records;
    };

    template<class T> inline T read_and_advance(char* buf, ssize_t& offset){
        T ret;
        memcpy(&ret, buf + offset, sizeof(T));
        offset += sizeof(T);
        return ret;
    }

    /**
     * Reads data from a buffer in big endian, with the given offset as a difference.
     * Advances offset by the size of T afterwards for the next read.
     * @tparam T The type that is to be read.
     * @param buf The source buffer.
     * @param offset The offset used. This will be advanced after the value is computed by sizeof(T).
     * @return The read value.
     */
    template<class T> inline T read_be_and_advance(char* buf, ssize_t& offset){
        T ret = read_big_endian<T>(buf + offset);
        offset += sizeof(T);
        return ret;
    }

    vector<RecordBatch> load_cluster_metadata();
}
