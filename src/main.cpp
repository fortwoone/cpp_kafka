#include "utils.hpp"


int main(int argc, char* argv[]) {
    // Disable output buffering
    cout << unitbuf;
    cerr << unitbuf;

    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        cerr << "Failed to create server socket: " << endl;
        return 1;
    }

    // Since the tester restarts your program quite often, setting SO_REUSEADDR
    // ensures that we don't run into 'Address already in use' errors
    int reuse = 1;
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0) {
        close(server_fd);
        cerr << "setsockopt failed: " << endl;
        return 1;
    }

    InternetSockAddr server_addr{};
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(9092);

    if (bind(server_fd, reinterpret_cast<SockAddrPtr>(&server_addr), sizeof(server_addr)) != 0) {
        close(server_fd);
        cerr << "Failed to bind to port 9092" << endl;
        return 1;
    }

    int connection_backlog = 5;
    if (listen(server_fd, connection_backlog) != 0) {
        close(server_fd);
        cerr << "listen failed" << endl;
        return 1;
    }

    cout << "Waiting for a client to connect...\n";

    InternetSockAddr client_addr{};
    socklen_t client_addr_len = sizeof(client_addr);

    cerr << "Logs from your program will appear here!\n";

    int client_fd = accept(server_fd, reinterpret_cast<SockAddrPtr>(&client_addr), &client_addr_len);
    cout << "Client connected\n";

    // Read request
    char buffer[1024];
    ssize_t bytes_read = recv(client_fd, buffer, sizeof(buffer), 0);
    if (bytes_read <= 0){
        cerr << "Couldn't read request, or client disconnected\n";
        close(client_fd);
        close(server_fd);
        return 1;
    }

    // Prepare response
    fint msg_size, correlation_id;

    // Extract correlation ID from the buffer.
    memcpy(&correlation_id, buffer + 8, sizeof(correlation_id));
    // Generate message size
    msg_size = host_to_network_long(sizeof(correlation_id));

    // Send response
    send(client_fd, &msg_size, sizeof(msg_size), 0);
    send(client_fd, &correlation_id, sizeof(correlation_id), 0);

    close(client_fd);
    close(server_fd);
    return 0;
}