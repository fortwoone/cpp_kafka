//
// Created by fortwoone on 14/09/2025.
//

#include "requests.hpp"

namespace cpp_kafka{
    void Response::send_to_client(int client_fd){
        fshort err_code_as_net = host_to_network_short(to_underlying(err_code));
        // 1 is added to this value because this is how the protocol defines the encoding.
        ubyte api_ver_size_as_net = static_cast<ubyte>(
            host_to_network_short(api_versions.size() + 1)
        );

        send(client_fd, &msg_size, sizeof(msg_size), 0);
        send(client_fd, &correlation_id, sizeof(correlation_id), 0);
        send(client_fd, &err_code_as_net, sizeof(err_code_as_net), 0);
        send(client_fd, &api_ver_size_as_net, sizeof(api_ver_size_as_net), 0);

        for (const auto& api_ver_entry: api_versions){
            fshort api_key_as_net = host_to_network_short(to_underlying(api_ver_entry.api_key));
            fshort api_minver_as_net = host_to_network_short(api_ver_entry.min_version);
            fshort api_maxver_as_net = host_to_network_short(api_ver_entry.max_version);
            ubyte api_tagbuf_as_net = static_cast<ubyte>(host_to_network_short(api_ver_entry.tag_buffer));
            send(client_fd, &api_key_as_net, sizeof(api_key_as_net), 0);
            send(client_fd, &api_minver_as_net, sizeof(api_minver_as_net), 0);
            send(client_fd, &api_maxver_as_net, sizeof(api_maxver_as_net), 0);
            send(client_fd, &api_tagbuf_as_net, sizeof(api_tagbuf_as_net), 0);
        }

        send(client_fd, &throttle_time_ms, sizeof(throttle_time_ms), 0);
    }

    int receive_request_from_client(int client_fd, Response& response, Request& request){
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

        cerr << "Request API Key: " << request.header.request_api_key << "\n";

        // Extract the request API version from the buffer.
        memcpy(
            &request.header.request_api_version,
            buffer + 6,
            sizeof(request.header.request_api_version)
        );

        cerr << "Request API Version: " << request.header.request_api_version << "\n";

        // Extract correlation ID from the buffer.
        memcpy(
            &response.correlation_id,
            buffer + 8,
            sizeof(response.correlation_id)
        );
        request.header.correlation_id = response.correlation_id;

        cerr << "Request Correlation ID: " << response.correlation_id << "\n";

        // Extract the client ID.
        // Get the size first.
        ushort cli_id_len;
        memcpy(
            &cli_id_len,
            buffer + 12,
            sizeof(cli_id_len)
        );
        request.header.client_id.resize(cli_id_len);
        // Extract the string contents.
        memcpy(
            request.header.client_id.data(),  // Edit directly into the string.
            buffer + 14,
            cli_id_len
        );

        cerr << "API Client ID: " << request.header.client_id << "\n";

        response.throttle_time_ms = 0;

        if (request.header.request_api_version > 4){
            response.err_code = KafkaErrorCode::UNSUPPORTED_VERSION;
        }
        else{
            response.err_code = KafkaErrorCode::NO_ERROR;
            response.api_versions.push_back(
                {
                    KafkaAPIKey::API_VERSIONS,
                    0,                        // Min. supported version
                    4                         // Max. supported version
                }
            );
        }

        // Compute the message size.
        response.msg_size = host_to_network_long(
            sizeof(KafkaErrorCode)
            + sizeof(response.correlation_id)
            + sizeof(APIVersionArrEntry) * response.api_versions.size()
            + sizeof(response.throttle_time_ms)
            + 2
        );
        return 0;
    }
}
