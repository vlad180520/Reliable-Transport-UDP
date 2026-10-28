#include <pthread.h>
#include <cstdlib>
#include <cstring>
#include <map>
#include <cstdint>
#include <algorithm>
#include "lib.h"
#include "utils.h"
#include <cassert>
#include <poll.h>
#include <sys/timerfd.h>
#include <sys/socket.h>
#include <netinet/in.h>

// Harta globala care asociaza fiecarui conn_id pointerul la structura sa de conexiune.
// Este accesata si de libcommon.cpp prin extern, de aceea nu e declarata static.
std::map<int, struct connection *> cons;

// Array de descriptori de fisier monitorizati de poll() pentru date primite pe socketi.
// Fiecare intrare corespunde unui socket de date al unei conexiuni.
struct pollfd data_fds[MAX_CONNECTIONS];

// Array de descriptori de fisier monitorizati de poll() pentru expirarea timerelor.
// Fiecare intrare corespunde unui timerfd asociat unei conexiuni, folosit pentru retransmisie.
struct pollfd timer_fds[MAX_CONNECTIONS];

// Numarul curent de conexiuni active (si implicit indexul urmatorului slot liber din arrays).
int fdmax = 0;

class Sender {
    private:

        // Viteza legaturii in Mbps, necesara pentru calculul ferestrei optime (BDP).
        // Static deoarece este setata o singura data in init_sender si folosita in setup_connection.
        static int g_speed_mbps;

        // Latenta link-ului in ms, folosita atat la calculul BDP cat si la setarea timeout-ului.
        // Static deoarece este setata o singura data in init_sender si folosita in setup_connection.
        static int g_delay_ms;

        // Contor care genereaza ID-uri unice pentru fiecare conexiune noua.
        // Static deoarece valoarea trebuie pastrata intre apeluri succesive la setup_connection.
        static int g_next_conn_id;

        // Incearca sa umple fereastra de trimitere cu segmente noi din send_buf.
        // Trebuie apelata intotdeauna cu mutex-ul conexiunii deja luat.
        static void pump_window(struct connection *connection)
        {
            // Calculam limita efectiva a ferestrei: minimul dintre fereastra locala (BDP)
            // si fereastra anuntata de receiver (flow control). Astfel respectam ambele constrangeri.
            int win = std::min(connection->max_window_seq, connection->recv_window_peer);

            // Cat timp mai avem loc in fereastra si avem date de trimis, construim si trimitem segmente.
            while ((int)connection->window_segs.size() < win && !connection->send_buf.empty()) {

                // Determinam cati bytes putem pune in acest segment: cel mult MAX_DATA_SIZE.
                int pay_len = std::min((int)connection->send_buf.size(), MAX_DATA_SIZE);

                // Alocam un buffer pentru segmentul complet: header + payload.
                std::vector<char> seg(sizeof(poli_tcp_data_hdr) + pay_len);

                // Interpretam inceputul bufferului ca header si il completam.
                poli_tcp_data_hdr *hdr = (poli_tcp_data_hdr *)seg.data();
                // Marcam segmentul ca apartinand protocolului nostru.
                hdr->protocol_id = POLI_PROTOCOL_ID;
                // Identificatorul conexiunii, util receiverului pentru demultiplexare.
                hdr->conn_id = (uint8_t)connection->conn_id;
                // Tipul segmentului: date utile.
                hdr->type = TYPE_DATA;
                // Numarul de secventa in network byte order (big-endian).
                hdr->seq_num = htons((uint16_t)connection->next_seq);
                // Lungimea payload-ului in network byte order.
                hdr->len = htons((uint16_t)pay_len);

                // Copiem exact pay_len bytes din coada send_buf in zona de payload a segmentului.
                for (int i = 0; i < pay_len; i++) {
                    seg[sizeof(poli_tcp_data_hdr) + i] = connection->send_buf.front();
                    // Eliminam fiecare byte consumat din coada.
                    connection->send_buf.pop_front();
                }

                // Trimitem segmentul UDP catre adresa si portul dedicat al receiverului.
                sendto(connection->sockfd, seg.data(), seg.size(), 0,
                    (struct sockaddr *)&connection->servaddr, sizeof(connection->servaddr));

                // Pastram o copie a segmentului in fereastra pentru a putea retransmite daca e nevoie.
                connection->window_segs[connection->next_seq] = seg;

                // Avansam la urmatorul numar de secventa.
                // Wrap-around la 65536 = 2^16 deoarece seq_num este uint16_t (16 biti),
                // deci poate reprezenta valori de la 0 la 65535, dupa care se intoarce la 0.
                connection->next_seq = (connection->next_seq + 1) % 65536;
            }
        }

        // Thread dedicat protocolului de trimitere. Ruleaza in paralel cu thread-ul principal.
        // Este static pentru a putea fi pasata ca functie de start la pthread_create.
        static void *sender_handler(void *arg)
        {
            // Codul de rezultat returnat de recv_message_or_timeout.
            int res = 0;
            // Buffer temporar in care citim segmentele (ACK-uri) primite de la receiver.
            char buf[MAX_SEGMENT_SIZE];

            while (1) {

                // Daca nu exista nicio conexiune inregistrata inca, nu avem ce procesa.
                if (cons.empty()) {
                    continue;
                }

                // Asteptam un eveniment pe oricare dintre socket-urile sau timerele active.
                // Functia intoarce: numarul de bytes cititi (ACK), -1 (timeout), -14 (niciun eveniment).
                int conn_id = -1;
                res = recv_message_or_timeout(buf, MAX_SEGMENT_SIZE, &conn_id);

                // Procesam evenimentul doar daca e un ACK sau un timeout (nu "niciun eveniment").
                if (res != -14) {
                    // Obtinem pointerul la conexiunea pentru care a aparut evenimentul.
                    struct connection *connection = cons[conn_id];
                    // Luam mutex-ul inainte de a modifica starea conexiunii.
                    pthread_mutex_lock(&connection->con_lock);

                    /* Handle segment received from the receiver. We use this between locks
                    as to not have synchronization issues with the send_data calls which are
                    on the main thread */

                    if (res == -1) {
                        // Timerul de retransmisie a expirat: retransmitem toate segmentele
                        // trimise dar inca neconfirmate (Selective Repeat ARQ).
                        for (auto &[seq, seg] : connection->window_segs)
                            sendto(connection->sockfd, seg.data(), seg.size(), 0,
                                (struct sockaddr *)&connection->servaddr, sizeof(connection->servaddr));

                    } else if (res > 0) {
                        // Am primit un ACK de la receiver; interpretam bufferul ca header de control.
                        poli_tcp_ctrl_hdr *ctrl = (poli_tcp_ctrl_hdr *)buf;

                        // Extragem numarul de confirmare (cumulative ACK) din network byte order.
                        int ack_num = ntohs(ctrl->ack_num);
                        // Extragem fereastra disponibila la receiver pentru flow control.
                        int recv_win = ntohs(ctrl->recv_window);

                        // Actualizam fereastra peer-ului cu valoarea din ACK.
                        connection->recv_window_peer = recv_win;

                        // Calculam cate segmente noi sunt confirmate de acest ACK.
                        // Adaugam 65536 inainte de modulo pentru a gestiona corect wrap-around-ul
                        // numerelor de secventa uint16_t (ex: ack_num=2, send_base=65534 => diff=4).
                        int diff = (ack_num - connection->send_base + 65536) % 65536;
                        // Acceptam ACK-ul doar daca confirma segmente noi si nu e un ACK duplicat.
                        // Limita 1000 protejeaza impotriva ACK-urilor vechi din cauza wrap-around-ului.
                        if (diff > 0 && diff < 1000) {
                            // Stergem din fereastra toate segmentele confirmate de acest ACK.
                            for (int s = connection->send_base; s != ack_num; s = (s + 1) % 65536)
                                connection->window_segs.erase(s);
                            // Avansam baza ferestrei la urmatorul segment neconfirmat.
                            connection->send_base = ack_num;
                        }
                    }

                    // Eliberam mutex-ul dupa ce am terminat de modificat starea conexiunii.
                    pthread_mutex_unlock(&connection->con_lock);
                }

                // Dupa fiecare eveniment (sau chiar daca nu a fost niciunul), incercam sa trimitem
                // date noi pentru toate conexiunile. Aceasta este calea prin care primele segmente
                // sunt trimise la inceput, inainte sa fi primit vreun ACK sau timeout.
                for (auto &kv : cons) {
                    struct connection *conn = kv.second;
                    // Luam mutex-ul pentru a proteja accesul la send_buf si window_segs.
                    pthread_mutex_lock(&conn->con_lock);
                    pump_window(conn);
                    pthread_mutex_unlock(&conn->con_lock);
                }
            }
        }

    public:

        int send_data(int conn_id, char *buffer, int len)
        {
            // Retinem numarul de bytes de scris pentru a-l putea returna la final.
            int size = len;

            // Luam mutex-ul pentru a nu scrie in send_buf in acelasi timp cu sender_handler.
            pthread_mutex_lock(&cons[conn_id]->con_lock);

            /* We will write code here as to not have sync problems with sender_handler */

            // Copiem toti bytes din bufferul aplicatiei in coada send_buf a conexiunii.
            // sender_handler va consuma acesti bytes si ii va impacheta in segmente DATA.
            for (int i = 0; i < size; i++) {
                cons[conn_id]->send_buf.push_back(buffer[i]);
            }

            // Eliberam mutex-ul dupa ce am terminat de scris in coada.
            pthread_mutex_unlock(&cons[conn_id]->con_lock);

            // Returnam numarul de bytes acceptati (mereu egal cu len, nu facem scriere partiala).
            return size;
        }

        int setup_connection(uint32_t ip, uint16_t port)
        {
            /* Implement the sender part of the Three Way Handshake. Blocks
            until the connection is established */

            // Alocam structura de conexiune; constructorul initializeaza mutex-ul si cond var-ul.
            struct connection *connection = new struct connection();
            // Cream socket UDP pentru aceasta conexiune.
            connection->sockfd = socket(AF_INET, SOCK_DGRAM, 0);
            // Verificam ca socket-ul a fost creat cu succes.
            assert(connection->sockfd >= 0);

            // Construim adresa serverului cu portul de listening specificat de apelant.
            struct sockaddr_in server_addr;
            memset(&server_addr, 0, sizeof(server_addr));
            // Folosim familia de adrese IPv4.
            server_addr.sin_family = AF_INET;
            // Adresa IP a serverului, deja in network byte order.
            server_addr.sin_addr.s_addr = ip;
            // Portul serverului, deja in network byte order.
            server_addr.sin_port = port;

            // Construim headerul SYN: pachet de control fara payload.
            poli_tcp_ctrl_hdr syn;
            memset(&syn, 0, sizeof(syn));
            // Marcam pachetul cu ID-ul protocolului nostru.
            syn.protocol_id = POLI_PROTOCOL_ID;
            // conn_id 0 la handshake: conexiunea nu are inca un ID alocat.
            syn.conn_id = 0;
            // Tipul SYN semnaleaza cererea de deschidere a conexiunii.
            syn.type = TYPE_SYN;

            // SYN-ACK are ca payload portul dedicat pe care serverul l-a deschis pentru noi.
            poli_tcp_synack synack;

            // Adresa sursa a SYN-ACK-ului primit (adresa serverului).
            struct sockaddr_in from_addr;
            socklen_t from_len = sizeof(from_addr);

            // Folosim poll() pentru a putea impune un timeout la asteptarea SYN-ACK.
            struct pollfd syn_pfd;
            // Monitorizam socket-ul nostru pentru date primite.
            syn_pfd.fd = connection->sockfd;
            syn_pfd.events = POLLIN;
            int n;
            while (1) {
                // Trimitem SYN catre adresa de listening a serverului.
                sendto(connection->sockfd, &syn, sizeof(syn), 0,
                    (struct sockaddr *)&server_addr, sizeof(server_addr));
                DEBUG_PRINT("SYN trimis\n");

                // Asteptam maxim 200ms un raspuns. Daca expira, retrimitem SYN (reteaua putea pierde pachetul).
                int ret = poll(&syn_pfd, 1, 200);
                if (ret > 0 && (syn_pfd.revents & POLLIN)) {
                    from_len = sizeof(from_addr);
                    // Citim raspunsul primit de la server.
                    n = recvfrom(connection->sockfd, &synack, sizeof(synack), 0,
                                (struct sockaddr *)&from_addr, &from_len);
                    // Iesim din bucla doar daca am primit un SYN-ACK valid.
                    if (n > 0 && synack.hdr.type == TYPE_SYN_ACK)
                        break;
                }
                // timeout sau pachet invalid: reincercam
            }
            // Verificam postconditions: trebuie sa fi primit un SYN-ACK valid.
            assert(n > 0);
            assert(synack.hdr.type == TYPE_SYN_ACK);

            // Extragem portul dedicat transmis de server in payload-ul SYN-ACK (din network byte order).
            uint16_t data_port = ntohs(synack.data_port);
            DEBUG_PRINT("SYN-ACK primit, port dedicat: %d\n", data_port);

            // Actualizam adresa serverului cu noul port dedicat: de acum inainte trimitem date acolo.
            server_addr.sin_port = htons(data_port);
            // Retinem adresa finala a serverului in structura conexiunii.
            connection->servaddr = server_addr;

            // Construim headerul ACK: raspunsul nostru la SYN-ACK.
            poli_tcp_ctrl_hdr ack;
            memset(&ack, 0, sizeof(ack));
            ack.protocol_id = POLI_PROTOCOL_ID;
            // conn_id 0 si la ACK: inca nu schimbam date, nu avem ID stabilit.
            ack.conn_id = 0;
            // Tipul ACK inchide handshake-ul three-way.
            ack.type = TYPE_ACK;

            // Trimitem ACK catre portul dedicat al serverului.
            sendto(connection->sockfd, &ack, sizeof(ack), 0,
                (struct sockaddr *)&server_addr, sizeof(server_addr));
            DEBUG_PRINT("ACK trimis, handshake complet\n");

            // Atribuim un ID unic si incrementam contorul pentru urmatoarea conexiune.
            connection->conn_id = g_next_conn_id++;

            // Ambele numere de secventa (send_base si next_seq) incep de la 0.
            connection->send_base = 0;
            connection->next_seq = 0;

            // Initializam fereastra peer-ului cu valoarea anuntata de server in SYN-ACK.
            connection->recv_window_peer = ntohs(synack.hdr.recv_window);

            // Calculam fereastra optima locala folosind Bandwidth-Delay Product:
            // BDP = bandwidth_bytes_per_sec * RTT_sec
            // RTT = 4 * delay_ms / 1000 deoarece avem doua hop-uri (client->router->server),
            // fiecare cu latenta dus si intors, deci 4 tranzitii de delay_ms.
            // Fereastra in segmente = BDP / MAX_DATA_SIZE (cate segmente pot fi "in zbor").
            long bdp_bytes = (long)(g_speed_mbps * 1000000 / 8) * (4 * g_delay_ms) / 1000;
            connection->max_window_seq = (int)(bdp_bytes / MAX_DATA_SIZE);
            // Garantam cel putin un segment in fereastra pentru ca protocolul sa functioneze.
            if (connection->max_window_seq < 1)
                connection->max_window_seq = 1;
            DEBUG_PRINT("BDP=%ld bytes, fereastra optima=%d segmente\n", bdp_bytes, connection->max_window_seq);

            /* Since we can have multiple connection, we want to know if data is available
            on the socket used by a given connection. We use POLL for this */

            // Inregistram socket-ul conexiunii in array-ul de poll pentru date primite (ACK-uri).
            data_fds[fdmax].fd = connection->sockfd;
            data_fds[fdmax].events = POLLIN;

            /* This creates a timer and sets it to trigger at the retransmit timeout.
            We use TIMEOUT_SEND which is calculated from the link delay. */

            // Cream un timer periodic pentru retransmisie, bazat pe latenta legaturii.
            timer_fds[fdmax].fd = timerfd_create(CLOCK_REALTIME, 0);
            timer_fds[fdmax].events = POLLIN;
            struct itimerspec spec;
            // TIMEOUT_SEND(delay_ms) calculeaza un timeout de retransmisie optim (~31ms la 2ms delay).
            long timeout_ms = TIMEOUT_SEND(g_delay_ms);
            // Setam atat first expiration cat si intervalul periodic la acelasi timeout.
            spec.it_value.tv_sec = timeout_ms / 1000;
            spec.it_value.tv_nsec = (timeout_ms % 1000) * 1000000L;
            spec.it_interval.tv_sec = timeout_ms / 1000;
            spec.it_interval.tv_nsec = (timeout_ms % 1000) * 1000000L;
            // Pornim timerul.
            timerfd_settime(timer_fds[fdmax].fd, 0, &spec, NULL);

            // Inregistram conexiunea in harta INAINTE de fdmax++ pentru a evita race condition:
            // sender_handler ar putea prelua un ACK pentru aceasta conexiune inainte ca ea
            // sa fie inserata in cons, cauzand acces invalid la pointer.
            cons.insert({connection->conn_id, connection});
            // Abia acum incrementam fdmax, facand conexiunea vizibila pentru sender_handler.
            fdmax++;

            DEBUG_PRINT("Conexiune stabilita cu id=%d\n", connection->conn_id);
            return connection->conn_id;
        }

        void init_sender(int speed, int delay)
        {
            // Salvam parametrii legaturii in variabilele globale pentru a fi folositi
            // ulterior in setup_connection la calculul ferestrei si al timeout-ului.
            g_speed_mbps = speed;
            g_delay_ms = delay;

            pthread_t thread1;
            int ret;

            // Pornim thread-ul de protocol in fundal; va rula pana la terminarea procesului.
            ret = pthread_create(&thread1, NULL, sender_handler, NULL);
            // Daca pthread_create esueaza, nu putem continua (nu mai avem thread de trimitere).
            assert(ret == 0);
        }
};

int Sender::g_speed_mbps = 8;
int Sender::g_delay_ms = 2;
int Sender::g_next_conn_id = 0;

static Sender sender;

int send_data(int conn_id, char *buffer, int len)
{
    return sender.send_data(conn_id, buffer, len);
}

int setup_connection(uint32_t ip, uint16_t port)
{
    return sender.setup_connection(ip, port);
}

void init_sender(int speed, int delay)
{
    sender.init_sender(speed, delay);
}
