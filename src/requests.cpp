//
// Created by fortwoone on 14/09/2025.
//

#include "requests.hpp"

namespace cpp_kafka{
    void Response::send_to_client(int client_fd){
        fshort err_code_as_net = host_to_network_short(to_underlying(err_code));

        send(client_fd, &msg_size, sizeof(msg_size), 0);
        send(client_fd, &header.correlation_id, sizeof(header.correlation_id), 0);
        send(client_fd, &err_code_as_net, sizeof(err_code_as_net), 0);
    }

    int receive_request_from_client(int client_fd, Response& request){
        char buffer[1024];
        ssize_t bytes_read = recv(client_fd, buffer, sizeof(buffer), 0);
        if (bytes_read <= 0){
            cerr << "Couldn't read request, or client disconnected\n";
            return 1;
        }

        // Extract the request API key from the buffer.
        memcpy(
            &request.header.request_api_key,
            buffer + 4,
            sizeof(request.header.request_api_key)
        );

        // Extract the request API version from the buffer.
        memcpy(
            &request.header.request_api_version,
            buffer + 6,
            sizeof(request.header.request_api_version)
        );

        // Extract correlation ID from the buffer.
        memcpy(
            &request.header.correlation_id,
            buffer + 8,
            sizeof(request.header.correlation_id)
        );

        if (request.header.request_api_version > 4){
            request.err_code = KafkaErrorCode::UNSUPPORTED_VERSION;
        }
        else{
            request.err_code = KafkaErrorCode::NO_ERROR;
        }

        // Compute the message size.
        request.msg_size = host_to_network_long(sizeof(KafkaErrorCode) + sizeof(request.header.correlation_id));
        return 0;
    }
}
