//
// Created by fortwoone on 17/09/2025.
//

#include "varint_type.hpp"
#include <iostream>

namespace cpp_kafka{
    varint_t varint_t::decode_and_advance(char* buf, ssize_t& offset){
        fint value = 0;
        ubyte byte = 0, shift = 0;

        while (true){
            memcpy(&byte, buf + offset + count, 1);
            value |= (byte & 0x7F) << shift;

            if (!(byte & 0x80)){
                // Stop reading if no more bytes follow for this varint.
                break;
            }
            shift += 7;
            if (shift >= 35){
                throw runtime_error("Encoded value is too large for a VARINT.");
            }
        }
        fint decoded_value = (value >> 1) ^ (-(value & 1));
        auto ret = varint_t(decoded_value);
        cerr << "Offset pre-computation: " << offset << "\n";
        cerr << "Decoded value required size: "<< ret.needed_size() << "\n";
        offset += ret.needed_size();
        cerr << "Offset post-computation: " << offset << "\n";
        return decoded_value;
    }

    vector<ubyte> varint_t::encode() const{
        vector<ubyte> ret;

        uint calc = static_cast<uint>(value);
        ubyte byte;

        do{
            byte = calc & 0x7F;
            calc >>= 7;

            if (calc > 0){
                // Set the MSB if more bytes should be added
                byte |= 0x80;
            }
            ret.push_back(byte);
        } while (calc > 0);

        return ret;
    }

    unsigned_varint_t unsigned_varint_t::decode_and_advance(char* buf, ssize_t& offset){
        uint value = 0;
        ubyte byte = 0, shift = 0;
        ubyte count = 0;

        while (true){
            memcpy(&byte, buf + offset + count, 1);
            value |= (byte & 0x7F) << shift;

            if (!(byte & 0x80)){
                // Stop reading if no more bytes follow for this varint.
                break;
            }

            shift += 7;
            if (shift >= 32){
                throw runtime_error("Encoded value is too large for an UNSIGNED_VARINT.");
            }
            count++;
        }
        offset += count;

        return value;  // Implicitly converted to varint.
    }

    vector<ubyte> unsigned_varint_t::encode() const{
        vector<ubyte> ret;

        uint calc = value;
        ubyte byte;

        do{
            byte = calc & 0x7F;
            calc >>= 7;

            if (calc > 0){
                // Set the MSB if more bytes should be added
                byte |= 0x80;
            }
            ret.push_back(byte);
        } while (calc > 0);

        return ret;
    }
}
