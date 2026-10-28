
#pragma once

#include <cstdint>
#include "utils.h"
#include "protocol.h"
#include <arpa/inet.h>
#include <pthread.h>
#include <deque>
#include <vector>
#include <map>

/* Maximum segment size, change as you see fit */
#define MAX_DATA_SIZE 512
#define MAX_SEGMENT_SIZE (MAX_DATA_SIZE + sizeof(poli_tcp_data_hdr))

#define MAX_CONNECTIONS 32

/* Protocol control block. Used track different parameters about a connection. 
 * Will need to be extenden to solve the homework with other parameters such as
 * last_ack or status depending on how you implement your protocol. */
struct connection {
    /* common window for both the sender and receiver. */
    /* list window: A window representation */
    int sockfd; /* socket used for this connection */
    int conn_id; /* connection identifier */
    struct sockaddr_in servaddr; /* used to identify the destination */
    pthread_mutex_t con_lock; /* Used for syncronization with the handler thread and read/send calls.*/

    /* TODO. Parameters used only by the sender */

    // Numarul maxim de segmente trimise dar neconfirmate simultan, calculat din BDP
    int max_window_seq; /* Used to store the max number of packets that can be inflight, since we can
                           have many more packets in our window */
    // Fereastra de receptie anuntata de receiver in ultimul ACK primit
    int recv_window_peer;
    // Numarul de secventa al celui mai vechi segment trimis dar neconfirmat
    int send_base;
    // Numarul de secventa al urmatorului segment de trimis
    int next_seq;
    // Coada de bytes pusa de send_data, consumata de sender_handler
    std::deque<char> send_buf;
    // Segmentele trimise dar neconfirmate, indexate dupa numarul de secventa, pentru retransmisie
    std::map<int, std::vector<char>> window_segs;

    /* TODO. Parameters used only by the client */

    // Urmatorul numar de secventa asteptat in ordine de catre receiver
    int expected_seq;
    // Bytes receptionati in ordine, gata sa fie livrati prin recv_data
    std::deque<char> recv_buf;
    // Segmente sosite out-of-order, stocate temporar (Selective Repeat)
    std::map<int, std::vector<char>> recv_window;
    // Variabila de conditie pe care recv_data asteapta cand recv_buf e gol
    pthread_cond_t data_ready;
};

/* ########## API that we expose to the application ########### */

/* Equivalent of listen. Ran by the server to waits for a connection from a
 * client. Returns a connection id. Blocking untill it receives a connection
 * request */
int wait4connect(uint32_t ip, uint16_t port);
/* Equivalent of connect. Used by the client to connect to a server. */
int setup_connection(uint32_t ip, uint16_t port);
/* Equivalent to recv. Blocking if there is no data to be written in buffer */
int recv_data(int connectionid, char *buffer, int len);
/* Equivalent to send. Used by the client to send a stream of bytes as segments */
int send_data(int conn_id, char *buffer, int len);
/* Used to initialize your protocol on the receiver side. */
void init_receiver(int recv_buffer_bytes);
/* Used to initialize your protocol on the sender side */
void init_sender(int speed, int delay);

/* ######### Internal API used by sender and receiver ########### */
int recv_message_or_timeout(char *buff, size_t len, int *conn_id);
