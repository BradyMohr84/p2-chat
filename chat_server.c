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

void broadcast(char *cmd_code, char *out_msg, int exclude_slot)
{
    //if the command is join 
    if(strcmp("JOIN", cmd_code) == 0) 
    {
        //lock for shared data
        pthread_mutex_lock(&client_lock);
        for(int i = 0; i < MAX_CLIENTS; i++)
        {
            //send to all clients that are active and have joined
            if(client_list[i].active && client_list[i].name[0] != '\0')
            {
                send_line(client_list[i].sock, out_msg);
            }
        }
         pthread_mutex_unlock(&client_lock);
    }
    else if (strcmp(cmd_code, "MSG") == 0 || strcmp(cmd_code, "LEAVE") == 0)
    {
        //lock for global data
        pthread_mutex_lock(&client_lock);
        for (int i = 0; i < MAX_CLIENTS; i++)
        {
            //send the msessage only if the client is active, has joined, and is not the sending client
            if (client_list[i].active && client_list[i].name[0] != '\0' && i != exclude_slot)
            {
                send_line(client_list[i].sock, out_msg);
            }
        }
        pthread_mutex_unlock(&client_lock);
    }
    

}

void handle_who(int fd)
{
    char reply[BUFFER_SIZE];
    int len = snprintf(reply, sizeof(reply), "GROUP");
    //lock for global data
    pthread_mutex_lock(&client_lock);
    for(int i = 0; i < MAX_CLIENTS; i++)
    {
        if(client_list[i].active && client_list[i].name[0] != '\0')
        {
            int n = snprintf(reply + len, sizeof(reply) - len," %s", client_list[i].name);
            if (n < 0 || n >= (int)(sizeof(reply) - len)) break;  // if the buffer is out of room 
            len += n;
        } 
    }
    pthread_mutex_unlock(&client_lock);
    send_line(fd, reply);
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

int handle_join(int slot, char *name)
{
    //if name is empty or has spaces reject it
    if(strlen(name) == 0 || strstr(name, " ") ||strlen(name) >= MAX_NAME) return -1;
    pthread_mutex_lock(&client_lock); // touches shared data so lock
    
    // if the location this client is destined for has a name
    // do not allow it to join
    if (client_list[slot].name[0] != '\0') 
    {
        pthread_mutex_unlock(&client_lock);
        return -1;
    }
    for(int i = 0; i < MAX_CLIENTS; i++)
    {
        //if slot is in use and the name is taken
        if(client_list[i].active && strcmp(name, client_list[i].name) == 0) 
        {
            pthread_mutex_unlock(&client_lock);
            return -1; 
        }
    }
    snprintf(client_list[slot].name, MAX_NAME, "%s", name);
    pthread_mutex_unlock(&client_lock);
    return 0;     
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
            char *cmd = line; // the command
            char *arg = ""; //the argument for the given command 
            char *space = strchr(line, ' '); //find the space between command and the name of the client
            if(space != NULL)
            {
                *space = '\0'; //make cmd only be the command
                arg = space + 1; // arg becomes the username
            }

            if(strcmp("JOIN", cmd) == 0) 
            {
                if (handle_join(sock_slot, arg) == 0) { //if the client joined
                    send_line(client_fd, "OK"); //Tell it join was OK

                    char out[BUFFER_SIZE];
                    snprintf(out, sizeof(out), "JOIN %s", arg); 
                    broadcast(cmd, out, -1); //Tell all other clients someone has joined
                }
                else {
                    send_line(client_fd, "BADNAME");
                }
            }
            //if client asked WHO
            else if(strcmp("WHO", cmd) == 0)
            {
                handle_who(client_fd);
            }
            else if (strcmp(cmd, "MSG") == 0)
            {
                //get the name of the client that is sending a message
                char myname[MAX_NAME];
                pthread_mutex_lock(&client_lock);
                snprintf(myname, sizeof(myname), "%s", client_list[sock_slot].name);
                pthread_mutex_unlock(&client_lock);

                //if the sending client has not joined
                if (myname[0] == '\0')
                {
                    send_line(client_fd, "ERROR must JOIN first");
                }
                //if the message is empty
                else if (arg[0] == '\0')
                {
                    send_line(client_fd, "ERROR empty message");
                }
                else
                {
                    char out[BUFFER_SIZE];
                    //create the string to send out
                    int n = snprintf(out, sizeof(out), "MSG %s %s", myname, arg);
                    if (n >= (int)sizeof(out))
                    {
                        send_line(client_fd, "ERROR message too long");
                    }
                    else
                    {
                        send_line(client_fd, "OK");
                        broadcast(cmd, out, sock_slot);
                    }
                }

            } 
            else if(strcmp(cmd, "LEAVE") == 0)
            {
                send_line(client_fd, "OK");
                break;
            }
            else
            {
                send_line(client_fd, "ERROR command not recognized"); 
            } 
        
        }
        // Close the current client connection
        printf("[*] Client disconnected. Cleaning up socket.\n");

        //global data lock needed
        char myname[MAX_NAME];
        pthread_mutex_lock(&client_lock);
        snprintf(myname, sizeof(myname), "%s", client_list[sock_slot].name);
        pthread_mutex_unlock(&client_lock);

        //Only brodcast if the leaving client has joined
        if (myname[0] != '\0')
        {
            char out[BUFFER_SIZE];
            snprintf(out, sizeof(out), "LEAVE %s", myname);
            broadcast("LEAVE", out, sock_slot);   // takes the lock itself
        }

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
