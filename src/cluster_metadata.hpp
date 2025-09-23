//
// Created by fortwoone on 17/09/2025.
//

#pragma once

#include <fcntl.h>
#include <fstream>
#include <iterator>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <variant>
#include <vector>

#include "utils.hpp"
#include "varint_type.hpp"
#include "payloads.hpp"

namespace cpp_kafka{
    using std::holds_alternative;
    using std::ifstream;
    using std::invalid_argument;
    using std::istreambuf_iterator;
    using std::out_of_range;
    using std::runtime_error;
    using std::string;
    using std::to_string;
    using std::unordered_map;
    using std::unordered_set;
    using std::variant;
    using std::vector;

    // Can be any of the types listed in the definition.
    using Payload = variant<vector<ubyte>, FeatureLevelPayload, TopicPayload, PartitionPayload>;

    struct RecordValue{
        PayloadHeader header;
        Payload payload;
    };

    /**
     * A record in a batch from the cluster metadata log file.
     */
    struct Record{
        varint_t length;                    // The record's length in bytes.
        [[maybe_unused]] ubyte attributes;  // The record's attributes, stored in a single byte. Currently unused by the protocol.
        varint_t timestamp_delta;           // The record's timestamp delta.
        varint_t offset_delta;              // The record's offset delta.
        varint_t key_length;                // The record's key string length.
        string key;                         // The record's key.
        varint_t value_length;              // The record's value length.
        RecordValue value;                  // Can be either a metadata record payload, or a regular value.

        [[nodiscard]] bool is_metadata() const{
            return !holds_alternative<vector<ubyte>>(value.payload);
        }

        /**
         * Check if this record holds a feature level payload.
         * @return true if it does, false otherwise.
         */
        [[nodiscard]] bool is_feature_level() const{
            if (!is_metadata()){
                return false;
            }
            return holds_alternative<FeatureLevelPayload>(value.payload);
        }

        /**
         * Check if this record holds a topic payload.
         * @return true if it does, false otherwise.
         */
        [[nodiscard]] bool is_topic() const{
            if (!is_metadata()){
                return false;
            }
            return holds_alternative<TopicPayload>(value.payload);
        }

        /**
         * Check if this record holds a partition payload.
         * @return true if it does, false otherwise.
         */
        [[nodiscard]] bool is_partition() const{
            if (!is_metadata()){
                return false;
            }
            return holds_alternative<PartitionPayload>(value.payload);
        }
    };

    /**
     * A record batch in the cluster metadata log file.
     */
    struct RecordBatch{
        flong base_offset;                      // The batch's base offset.
        fint batch_length;                      // The batch's length in bytes.
        uint partition_leader_epoch;            // The batch's partition leader epoch.
        ubyte magic;                            // The batch's magic byte. Usually set to 2.
        fint crc_checksum;                      // The batch's CRC checksum.
        fshort attributes;                      // The batch's attribute bitfield.
        fint last_offset_delta;                 // The batch's last offset delta.
        flong base_timestamp,                   // The batch's base timestamp (Unix format, in milliseconds).
              max_timestamp;                    // The batch's maximum timestamp (Unix format, in milliseconds).
        flong producer_id;                      // The batch's producer ID. Set to -1 if there isn't one.
        fshort producer_epoch;                  // The batch's producer epoch. Set to -1 if there is no producer.
        fint base_sequence;                     // The batch's base sequence.
        vector<Record> records;                 // The records contained in this batch.
    };

    /**
     * Check if the given UUID refers to a topic.
     * @param uuid The UUID to match against a topic.
     * @return true of it does, false otherwise.
     */
    bool topic_exists_as_uuid(const UUID& uuid);

    string get_topic_name_from_uuid(const UUID& uuid);

    /**
     * Return all the partitions for a topic UUID.
     * @param uuid The topic UUID.
     * @return A list of partition payloads read from cluster metadata.
     * @throw out_of_range if the UUID does not refer to a topic.
     */
    vector<PartitionPayload> get_partitions_for_uuid(const UUID& uuid);

    /**
     * Count how many partitions exist for the given topic UUID.
     * @param uuid The topic UUID.
     * @return The partition list's size.
     */
    static size_t get_partition_count_for_uuid(const UUID& uuid);

    /**
     * Reads a record batch from a topic's log file using its UUID and the partition index.
     * @param topic_uuid The topic's UUID.
     * @param partition The partition index for the given topic.
     * @return The record batch as a raw byte array, read from the corresponding log file.
     * @throw invalid_argument if the UUID doesn't refer to a topic.
     * @throw out_of_range if the partition index is bigger than or equal to the number of available partitions for this topic.
     * @throw runtime_error if the log file couldn't be opened for any reason.
     */
    vector<ubyte> get_raw_record_batch(const UUID& topic_uuid, const fint& partition);

    vector<RecordBatch> get_record_batches_from_topic(const string& topic_name, const fint& partition, vector<ubyte>* raw_byte_arr = nullptr);

    /**
     * Load all record batches from the cluster metadata.
     * @return A vector containing all loaded batches.
     */
    vector<RecordBatch> load_cluster_metadata();
}
