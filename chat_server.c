#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include "port_utils.h"
#include <pthread.h>
#include <signal.h>
#include <sys/socket.h>
#include <stdint.h>

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

//used to send messages to the clients according to the CRLF requirments
int send_line(int fd, const char *text)
{
   char out[BUFFER_SIZE + 3];
   int len = snprintf(out, sizeof(out), "%s\r\n", text);
    if (len < 0 || len >= (int)sizeof(out)) return -1;
    return write(fd, out, len) < 0 ? -1 : 0;
}

int read_line(int fd, char *buf, int *len, char *line, int line_size)
{
    while(1)
    {
        //look throught the line sent to the server to ensure it is valid
        for(int i = 0; i+1 < *len; i++)
        {
            //if line is valid
            if (buf[i] == '\r' && buf[i + 1] == '\n') {
                if(i >= line_size) return -1; //line too big for buffer
                memcpy(line, buf, i); //copy the clients line to the server
                line[i] = '\0';
                //if more than one message in the buffer
                //move the next one up to make space
                memmove(buf, buf + i + 2, *len - i - 2);
                *len -= i + 2;
                return i;
            }
        }

    
        //if the buffer is full and the line has not ended
        if (*len >= BUFFER_SIZE) return -1;

        //read from the client
        int n = read(fd, buf + *len, BUFFER_SIZE - *len);
        if (n <= 0) return -1;
        *len += n;
    }
}

void *chat(void* arg)
{
    int sock_slot = (int)(intptr_t) arg;
    int client_fd = client_list[sock_slot].sock;
    char buf[BUFFER_SIZE];
    int buf_len = 0;
    char line[BUFFER_SIZE];
    while (read_line(client_fd, buf, &buf_len, line, sizeof(line)) >= 0)
        {
        printf("[->] %s\n", line);
        }
        // Close the current client connection
        printf("[*] Client disconnected. Cleaning up socket.\n");

        //lock the thread so issues with the global array do not happen when
        //a client disconnects
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
    signal(SIGPIPE, SIG_IGN);
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
         //lock the thread so issues with the global array do not happen
        pthread_mutex_lock(&client_lock);
        for (int i = 0; i < MAX_CLIENTS; i++) {
            //populate the first empty slot in the global array
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
            //lock the thread so issues with the global array do not happen
            //deactivate the slot in the array so it can be used by some other
            //client if the thread creation fails somehow
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
