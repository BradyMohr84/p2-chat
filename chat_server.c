#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include "port_utils.h"
#include <pthread.h>
#include <signal.h>
#include <sys/socket.h>

#define BUFFER_SIZE 1024
#define MAX_CLIENTS 64
#define MAX_NAME 32

typedef struct  {
    char name[MAX_NAME];
    int sock;
    int active;
} Client;

Client client_list[MAX_CLIENTS];
pthread_mutex_t client_lock = PTHREAD_MUTEX_INITIALIZER;

void *chat(void* arg)
{
    char buffer[BUFFER_SIZE];
    int n;
    int sock_slot = (int) arg;
    int client_fd = client_list[sock_slot].sock;
    // Chat loop: Handle the currently connected client
        while (1) {
            memset(buffer, 0, BUFFER_SIZE);
	    // Read client request
            n = read(client_fd, buffer, BUFFER_SIZE - 1);
            if (n <= 0) {
                break;
            }

            printf("[->] Received from client: %s", buffer);

            // Write back answer
            if (write(client_fd, buffer, n) < 0) {
                perror("[-] Write failed");
                break;
            }
        }
        // Close the current client connection
        printf("[*] Client disconnected. Cleaning up socket.\n");
        pthread_mutex_lock(&client_lock);
        close(client_fd);
        client_list[sock_slot].active = 0;
        client_list[sock_slot].name[0] = '\0';
        pthread_mutex_unlock(&client_lock);
        pthread_exit(0);
}

int main(int argc, char *argv[]) {
    int server_fd, client_fd;
    struct sockaddr_in server_addr, client_addr;
    socklen_t addr_size;
    
    
    
    int port;

    // Check command line arguments
    if (argc > 2) {
        fprintf(stderr, "Usage: %s [optional_port]\n", argv[0]);
        exit(1);
    } else if (argc == 2) {
        // Override with user provided port
        port = atoi(argv[1]);
    } else {
        // Fetch default port from utility function
        char *port_str = get_port();
        port = atoi(port_str);
    }

    // Create the socket
    server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        perror("[-] Socket creation failed");
        exit(1);
    }

    // Allow socket reuse if server is killed
    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    // Fill in the sockaddr struct
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port);
    server_addr.sin_addr.s_addr = INADDR_ANY;

    // Bind the socket
    if (bind(server_fd, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        perror("[-] Bind failed");
        close(server_fd);
        exit(1);
    }

    // Listen
    if (listen(server_fd, 5) < 0) {
        perror("[-] Listen failed");
        close(server_fd);
        exit(1);
    }
    printf("[*] Server listening on port %d...\n", port);

    // Infinite loop: accept new clients forever
    while (1) {
        printf("[*] Waiting for an incoming client connection...\n");

        addr_size = sizeof(client_addr);
        client_fd = accept(server_fd, (struct sockaddr*)&client_addr, &addr_size);
        if (client_fd < 0) {
            perror("[-] Accept failed");
            continue; // Keep server alive even if a single accept fails
        }

        //when a client connects, save its information to the list of clients
        int slot = -1;
        pthread_mutex_lock(&client_lock);
        for (int i = 0; i < MAX_CLIENTS; i++) {
            if (client_list[i].active == 0) {
                client_list[i].sock = client_fd;
                client_list[i].active = 1;
                client_list[i].name[0] = '\0'; 
                slot = i;
                break;
            }
        }
        pthread_mutex_unlock(&client_lock);
        if (slot < 0) {
            close(client_fd);
            continue;
        }

        printf("[+] Connected established with %s:%d\n",
               inet_ntoa(client_addr.sin_addr), ntohs(client_addr.sin_port));

        //give this connection to a thread
        pthread_t tid;
        int rc = pthread_create(&tid, NULL, chat, (void *)(intptr_t)slot);
        if (rc != 0) {
            fprintf(stderr, "[-] pthread_create failed: %s\n", strerror(rc));
            pthread_mutex_lock(&client_lock);
            client_list[slot].active = 0;
            client_list[slot].name[0] = '\0';
            pthread_mutex_unlock(&client_lock);
            close(client_fd);
            continue;
        }
        pthread_detach(tid);
    }
    // Unreachable code, but good practice
    close(server_fd);
    return 0;
}
