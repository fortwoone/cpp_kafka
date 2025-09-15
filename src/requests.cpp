//
// Created by fortwoone on 14/09/2025.
//

#include "requests.hpp"

namespace cpp_kafka{
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

    int receive_request_from_client(int client_fd, Response& response, Request& request){
        char buffer[1024];
        ssize_t bytes_read = recv(client_fd, buffer, sizeof(buffer), 0);
        if (bytes_read <= 0){
            cerr << "Couldn't read request, or client disconnected\n";
            return 1;
        }

        // Extract the request API key from the buffer.
        request.set_api_key(read_big_endian<fshort>(buffer + 4));  // IGNORE

        cerr << "Request API Key: " << request.get_api_key() << "\n";

        // Extract the request API version from the buffer.
        request.set_api_version(read_big_endian<fshort>(buffer + 6));

        cerr << "Request API Version: " << request.get_api_version() << "\n";

        // Extract correlation ID from the buffer.
        response.set_correlation_id(read_big_endian<fint>(buffer + 8));
        request.set_correlation_id(response.get_correlation_id());

        cerr << "Request Correlation ID: " << response.get_correlation_id() << "\n";

        fshort version = request.get_api_version();
        if (version >= 0 && version <= 4){
            response.append(host_to_network_short(to_underlying(KafkaErrorCode::NO_ERROR)));    // Error code
            response.append(static_cast<ubyte>(2));                                             // Length of API version entries + 1
            response.append(host_to_network_short(to_underlying(KafkaAPIKey::API_VERSIONS)));   // API key
            response.append(host_to_network_short(0));                                          // Min. version
            response.append(host_to_network_short(4));                                          // Max. version
            response.append(static_cast<ubyte>(0));                                             // Tag buffer for API version entry
            response.append(host_to_network_long(0));                                           // Throttle time (ms)
            response.append(static_cast<ubyte>(0));                                             // Tag buffer for throttle field.
        }
        else{
            response.append(host_to_network_short(to_underlying(KafkaErrorCode::UNSUPPORTED_VERSION))); // Error code
        }

        return 0;
    }
}
