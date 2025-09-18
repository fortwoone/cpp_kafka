//
// Created by fortwoone on 17/09/2025.
//

#include "varint_type.hpp"

namespace cpp_kafka{
    varint_t varint_t::decode_and_advance(char* buf, ssize_t& offset){
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
                throw runtime_error("Encoded value is too large for a VARINT.");
            }
            count++;
        }
        offset += count;

        return value;  // Implicitly converted to varint.
    }

    vector<ubyte> varint_t::encode() const{
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
