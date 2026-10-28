#pragma once

#include <cstdint>

/* Values used for protocol ID and type. */
#define POLI_PROTOCOL_ID 42

// Pasul 1 din handshake: clientul cere deschiderea unei conexiuni.
#define TYPE_SYN 0x01
// Pasul 2 din handshake: serverul accepta si trimite portul dedicat de date.
#define TYPE_SYN_ACK 0x02
// Pasul 3 din handshake: clientul confirma, conexiunea este stabilita.
#define TYPE_ACK 0x03
// Segment cu date utile trimis de la sender la receiver.
#define TYPE_DATA 0x04
// Pachet de confirmare (ACK) trimis de receiver inapoi la sender.
#define TYPE_CTRL 0x05

// Portul pe care serverul asculta cereri noi de conexiune.
#define SERVER_PORT 8032

/* Header for Data segments. Must be used in your implementation as it is. */
struct __attribute__((packed)) poli_tcp_data_hdr {
    uint8_t protocol_id;
    uint8_t conn_id;
    uint8_t type;
    uint16_t seq_num;
    uint16_t len;
};

/* Header for Control segments. Must be used in your implementation as it is. */
struct __attribute__((packed)) poli_tcp_ctrl_hdr {
    uint8_t protocol_id;
    uint8_t conn_id;
    uint8_t type;
    uint16_t ack_num;
    uint16_t recv_window;
};

// Segmentul SYN-ACK: header de control urmat de portul dedicat de date al serverului.
struct __attribute__((packed)) poli_tcp_synack {
    poli_tcp_ctrl_hdr hdr;
    uint16_t data_port;
};
