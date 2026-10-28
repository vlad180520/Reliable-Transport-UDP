#include <pthread.h>
#include <cstdlib>
#include <cstring>
#include <map>
#include <cstdint>
#include <algorithm>
#include "lib.h"
#include "utils.h"
#include <poll.h>
#include <cassert>
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
// Fiecare intrare corespunde unui timerfd asociat unei conexiuni.
struct pollfd timer_fds[MAX_CONNECTIONS];

// Numarul curent de conexiuni active (si implicit indexul urmatorului slot liber din arrays).
int fdmax = 0;

class Receiver {

    private:

        // Socket-ul de listening legat la portul SERVER_PORT (8032), pe care asteptam SYN de la clienti.
        // Static deoarece este unic per unitate de compilare si accesat de metode statice.
        static int listen_sock;

        // Contor care genereaza ID-uri unice pentru fiecare conexiune noua.
        // Static deoarece valoarea trebuie pastrata intre apeluri succesive la wait4connect.
        static int g_next_conn_id;

        // Thread dedicat protocolului de receptie. Ruleaza in paralel cu thread-ul principal.
        // Este static pentru a putea fi pasata ca functie de start la pthread_create.
        static void *receiver_handler(void *arg)
        {
            // Buffer temporar in care citim segmentele DATA primite de la sender.
            char segment[MAX_SEGMENT_SIZE];
            // Codul de rezultat returnat de recv_message_or_timeout.
            int res;
            DEBUG_PRINT("Starting receiver handler\n");

            while (1) {

                // Asteptam un eveniment pe oricare socket sau timer activ.
                // Blocam in do-while pana obtinem ceva altceva decat "niciun eveniment" (-14).
                int conn_id = -1;
                do {
                    res = recv_message_or_timeout(segment, MAX_SEGMENT_SIZE, &conn_id);
                } while (res == -14);

                // Daca conn_id-ul returnat nu exista in harta, ignoram evenimentul.
                // Poate aparea in fereastra de timp dintre fdmax++ si cons.insert (race condition).
                if (cons.find(conn_id) == cons.end())
                    continue;

                // Obtinem pointerul la conexiunea pentru care a aparut evenimentul.
                struct connection *connection = cons[conn_id];

                // Luam mutex-ul inainte de a citi sau modifica starea conexiunii.
                pthread_mutex_lock(&connection->con_lock);

                /* Handle segment received from the sender. We use this between locks
                as to not have synchronization issues with the recv_data calls which are
                on the main thread */

                if (res > 0) {
                    // Am primit un segment; interpretam bufferul ca header de date.
                    poli_tcp_data_hdr *hdr = (poli_tcp_data_hdr *)segment;

                    // Ignoram pachetele care nu apartin protocolului nostru sau nu sunt de tip DATA.
                    // Pot aparea pachete reziduale din handshake (ACK) pe socket-ul de date.
                    if (hdr->protocol_id != POLI_PROTOCOL_ID || hdr->type != TYPE_DATA) {
                        pthread_mutex_unlock(&connection->con_lock);
                        continue;
                    }

                    // Extragem numarul de secventa din network byte order.
                    int seq = ntohs(hdr->seq_num);
                    // Extragem lungimea payload-ului din network byte order.
                    int pay_len = ntohs(hdr->len);
                    // Pointerul la payload incepe imediat dupa header.
                    char *payload = segment + sizeof(poli_tcp_data_hdr);

                    DEBUG_PRINT("[recv] DATA seq=%d len=%d expected=%d\n", seq, pay_len, connection->expected_seq);

                    // Stocam segmentul in fereastra de receptie daca nu l-am
                    // mai primit. Verificarea evita duplicate.
                    if (connection->recv_window.find(seq) == connection->recv_window.end()) {
                        connection->recv_window[seq] = std::vector<char>(payload, payload + pay_len);
                    }

                    // Livram in ordine toate segmentele consecutive disponibile in fereastra.
                    // Ne oprim la primul segment lipsa din secventa.
                    while (connection->recv_window.find(connection->expected_seq) != connection->recv_window.end()) {
                        std::vector<char> &data = connection->recv_window[connection->expected_seq];

                        // Copiem datele segmentului in recv_buf, de unde le va citi recv_data.
                        for (char c : data)
                            connection->recv_buf.push_back(c);

                        // Stergem slotul livrat din fereastra pentru a elibera memoria.
                        connection->recv_window.erase(connection->expected_seq);

                        // Avansam la urmatorul numar de secventa asteptat.
                        // Wrap-around la 65536 = 2^16 deoarece seq_num este uint16_t (16 biti),
                        // deci poate reprezenta valori de la 0 la 65535, dupa care se intoarce la 0.
                        connection->expected_seq = (connection->expected_seq + 1) % 65536;
                    }

                    // Semnalizam recv_data ca au aparut date noi in recv_buf; aceasta poate
                    // fi blocata in pthread_cond_wait si trebuie trezita.
                    pthread_cond_signal(&connection->data_ready);

                    // Construim ACK-ul pe care il trimitem inapoi senderului.
                    poli_tcp_ctrl_hdr ack;
                    // Marcam pachetul cu ID-ul protocolului.
                    ack.protocol_id = POLI_PROTOCOL_ID;
                    // Identificatorul conexiunii, necesar senderului pentru demultiplexare.
                    ack.conn_id = (uint8_t)connection->conn_id;
                    // Tipul CTRL indica un pachet de control (ACK).
                    ack.type = TYPE_CTRL;
                    // ack_num = expected_seq: confirma toate segmentele cu seq < expected_seq.
                    ack.ack_num = htons((uint16_t)connection->expected_seq);
                    // Anuntam senderului cate segmente mai putem accepta (flow control).
                    // Valoarea 64 este fixa; o implementare mai avansata ar folosi spatiul real din recv_buf.
                    ack.recv_window = htons((uint16_t)64);

                    // Trimitem ACK catre adresa senderului (retinuta in servaddr la handshake).
                    sendto(connection->sockfd, &ack, sizeof(ack), 0,
                        (struct sockaddr *)&connection->servaddr, sizeof(connection->servaddr));

                    DEBUG_PRINT("[recv] ACK trimis ack_num=%d\n", connection->expected_seq);
                }
                // La timeout nu avem nimic de facut pe receiver: nu retransmitem nimic,
                // ACK-ul va fi retrimis la urmatorul segment DATA primit.

                // Eliberam mutex-ul dupa ce am terminat de modificat starea conexiunii.
                pthread_mutex_unlock(&connection->con_lock);
            }
        }

    public:

        int recv_data(int conn_id, char *buffer, int len)
        {
            // Obtinem pointerul la conexiunea identificata prin conn_id.
            struct connection *connection = cons[conn_id];

            // Luam mutex-ul pentru a nu citi din recv_buf in acelasi timp cu receiver_handler.
            pthread_mutex_lock(&connection->con_lock);

            /* We will write code here as to not have sync problems with recv_handler */

            // Asteptam pana cand recv_buf contine cel putin un byte.
            // pthread_cond_wait elibereaza atomic mutex-ul si suspenda thread-ul curent;
            // receiver_handler il va semnaliza cu pthread_cond_signal cand adauga date.
            while (connection->recv_buf.empty())
                pthread_cond_wait(&connection->data_ready, &connection->con_lock);

            // Copiem in bufferul aplicatiei cat putem fara a depasi len sau dimensiunea recv_buf.
            int to_copy = std::min(len, (int)connection->recv_buf.size());
            for (int i = 0; i < to_copy; i++) {
                buffer[i] = connection->recv_buf.front();
                // Eliminam fiecare byte livrat din coada.
                connection->recv_buf.pop_front();
            }

            // Eliberam mutex-ul dupa ce am terminat de citit din coada.
            pthread_mutex_unlock(&connection->con_lock);

            // Returnam numarul de bytes livrati aplicatiei.
            return to_copy;
        }

        int wait4connect(uint32_t ip, uint16_t port)
        {
            /* Implement the Three Way Handshake on the receiver part. This blocks
            * until a connection is established. */

            /* Receive SYN on the connection socket. Create a new socket and bind it to
            * the chosen port. Send the data port number via SYN-ACK to the client */

            // Buffer temporar pentru pachetele de handshake.
            char buf[MAX_SEGMENT_SIZE];
            // Adresa clientului, completata de recvfrom.
            struct sockaddr_in client_addr;
            socklen_t client_len = sizeof(client_addr);

            // Blocam pana primim un SYN de la un client; retinem si adresa sursa a clientului.
            int n = recvfrom(listen_sock, buf, sizeof(buf), 0,
                            (struct sockaddr *)&client_addr, &client_len);
            // Verificam ca am citit ceva.
            assert(n > 0);

            // Interpretam bufferul ca header de control si verificam ca e intr-adevar un SYN.
            poli_tcp_ctrl_hdr *syn = (poli_tcp_ctrl_hdr *)buf;
            assert(syn->protocol_id == POLI_PROTOCOL_ID);
            assert(syn->type == TYPE_SYN);
            DEBUG_PRINT("[recv] SYN primit de la client\n");


            // Alocam structura de conexiune; constructorul initializeaza mutex-ul si cond var-ul.
            struct connection *connection = new struct connection();

            // Cream un socket UDP nou, dedicat exclusiv acestei conexiuni.
            // Nu folosim listen_sock pentru date deoarece trebuie sa putem gestiona mai multi clienti.
            connection->sockfd = socket(AF_INET, SOCK_DGRAM, 0);
            assert(connection->sockfd >= 0);

            // Legam socket-ul la un port arbitrar ales de OS (portul 0 = alegere automata).
            struct sockaddr_in my_addr;
            memset(&my_addr, 0, sizeof(my_addr));
            my_addr.sin_family = AF_INET;
            // Acceptam conexiuni pe orice interfata de retea disponibila.
            my_addr.sin_addr.s_addr = INADDR_ANY;
            // Portul 0 inseamna ca OS-ul alege un port liber din intervalul efemer.
            my_addr.sin_port = 0;
            assert(bind(connection->sockfd, (struct sockaddr *)&my_addr, sizeof(my_addr)) == 0);

            // Aflam ce port a ales OS-ul cu getsockname, pentru a-l comunica clientului.
            socklen_t slen = sizeof(my_addr);
            getsockname(connection->sockfd, (struct sockaddr *)&my_addr, &slen);
            // Convertim portul din network byte order in host byte order pentru afisare/stocare.
            uint16_t data_port = ntohs(my_addr.sin_port);
            DEBUG_PRINT("[recv] socket dedicat pe portul %d\n", data_port);

            // Retinem adresa clientului ca destinatie pentru viitoarele ACK-uri de date.
            connection->servaddr = client_addr;

            // SYN-ACK are un payload special: portul dedicat pe care clientul trebuie sa trimita date.
            poli_tcp_synack synack;

            // Completam headerul SYN-ACK.
            synack.hdr.protocol_id = POLI_PROTOCOL_ID;
            // conn_id 0 la handshake: conexiunea nu are inca un ID alocat.
            synack.hdr.conn_id = 0;
            // Tipul SYN_ACK semnaleaza acceptarea conexiunii.
            synack.hdr.type = TYPE_SYN_ACK;
            // ack_num 0: nu confirmam niciun segment de date inca.
            synack.hdr.ack_num = 0;
            // Anuntam clientului fereastra noastra initiala de receptie (flow control).
            synack.hdr.recv_window = htons(64);
            // Trimitem portul dedicat in network byte order.
            synack.data_port = htons(data_port);

            // Folosim poll() pe socket-ul dedicat pentru a detecta ACK-ul clientului cu timeout.
            struct pollfd ack_pfd;
            ack_pfd.fd = connection->sockfd;
            ack_pfd.events = POLLIN;

            while (1) {
                // Trimitem SYN-ACK prin socket-ul de listening (clientul inca nu stie de portul dedicat).
                sendto(listen_sock, &synack, sizeof(synack), 0,
                    (struct sockaddr *)&client_addr, client_len);
                DEBUG_PRINT("[recv] SYN-ACK trimis\n");

                // Asteptam maxim 200ms ACK-ul pe socket-ul dedicat.
                // Daca expira, retrimitem SYN-ACK (reteaua poate pierde pachete).
                int ret = poll(&ack_pfd, 1, 200);
                if (ret > 0 && (ack_pfd.revents & POLLIN)) {
                    // Folosim MSG_PEEK pentru a vedea tipul pachetului fara a-l scoate din buffer.
                    // Astfel, daca e DATA, receiver_handler il poate citi mai tarziu.
                    poli_tcp_ctrl_hdr peek;
                    recvfrom(connection->sockfd, &peek, sizeof(peek), MSG_PEEK, NULL, NULL);

                    if (peek.type == TYPE_ACK) {
                        // ACK normal: handshake complet. Consumam pachetul si iesim.
                        recvfrom(connection->sockfd, buf, sizeof(buf), 0,
                                (struct sockaddr *)&client_addr, &client_len);
                        break;
                    } else if (peek.type == TYPE_DATA) {
                        // ACK-ul s-a pierdut in retea, dar clientul a inceput sa trimita date.
                        // Tratam primul segment DATA ca o confirmare implicita a handshake-ului.
                        // nu consumam pachetul; il lasam in buffer pentru receiver_handler.
                        break;
                    }
                    // Alt tip de pachet: il consumam si ignoram, continuam sa asteptam ACK.
                    recvfrom(connection->sockfd, buf, sizeof(buf), 0,
                            (struct sockaddr *)&client_addr, &client_len);
                }
            }
            DEBUG_PRINT("[recv] ACK primit, handshake complet\n");

            // Atribuim un ID unic si incrementam contorul pentru urmatoarea conexiune.
            connection->conn_id = g_next_conn_id++;
            // Primul segment de date asteptat are numarul de secventa 0.
            connection->expected_seq = 0;

            /* Since we can have multiple connection, we want to know if data is available
            on the socket used by a given connection. We use POLL for this */

            // Inregistram socket-ul conexiunii in array-ul de poll pentru segmente DATA primite.
            data_fds[fdmax].fd = connection->sockfd;
            data_fds[fdmax].events = POLLIN;

            /* This creates a timer and sets it to trigger every 1 sec. We use this
            to know if a timeout has happend on a connection */

            // Cream un timer periodic de 1 secunda. Pe receiver il folosim doar ca semnal
            // de "tick" in recv_message_or_timeout, nu avem retransmisii de facut.
            timer_fds[fdmax].fd = timerfd_create(CLOCK_REALTIME, 0);
            timer_fds[fdmax].events = POLLIN;
            struct itimerspec spec;
            spec.it_value.tv_sec = 1;
            spec.it_value.tv_nsec = 0;
            spec.it_interval.tv_sec = 1;
            spec.it_interval.tv_nsec = 0;
            // Pornim timerul.
            timerfd_settime(timer_fds[fdmax].fd, 0, &spec, NULL);

            // Inregistram conexiunea in harta INAINTE de fdmax++ pentru a evita race condition:
            // receiver_handler ar putea sa preia un pachet DATA pentru aceasta conexiune
            // inainte ca ea sa fie inserata in cons, cauzand acces invalid la pointer.
            cons.insert({connection->conn_id, connection});
            // Abia acum incrementam fdmax, facand conexiunea vizibila pentru receiver_handler.
            fdmax++;

            DEBUG_PRINT("[recv] conexiune inregistrata cu id=%d\n", connection->conn_id);
            return connection->conn_id;
        }

        void init_receiver(int recv_buffer_bytes)
        {
            // Cream socket-ul de listening pe care asteptam cereri SYN de la clienti.
            listen_sock = socket(AF_INET, SOCK_DGRAM, 0);
            assert(listen_sock >= 0);

            // Permitem reutilizarea portului imediat dupa un restart al serverului,
            // evitand eroarea "Address already in use" datorata TIME_WAIT.
            int opt = 1;
            setsockopt(listen_sock, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

            // Legam socket-ul la portul cunoscut SERVER_PORT (8032) pe toate interfetele.
            struct sockaddr_in addr;
            memset(&addr, 0, sizeof(addr));
            addr.sin_family = AF_INET;
            addr.sin_addr.s_addr = INADDR_ANY;
            addr.sin_port = htons(SERVER_PORT);
            assert(bind(listen_sock, (struct sockaddr *)&addr, sizeof(addr)) == 0);

            DEBUG_PRINT("[recv] listening pe portul %d\n", SERVER_PORT);

            pthread_t thread1;
            int ret;

            // Pornim thread-ul de protocol in fundal; va rula pana la terminarea procesului.
            ret = pthread_create(&thread1, NULL, receiver_handler, NULL);
            // Daca pthread_create esueaza, nu putem continua (nu mai avem thread de receptie).
            assert(ret == 0);
        }
};

int Receiver::listen_sock = -1;
int Receiver::g_next_conn_id = 0;

static Receiver receiver;

int recv_data(int conn_id, char *buffer, int len)
{
    return receiver.recv_data(conn_id, buffer, len);
}

int wait4connect(uint32_t ip, uint16_t port)
{
    return receiver.wait4connect(ip, port);
}

void init_receiver(int recv_buffer_bytes)
{
    receiver.init_receiver(recv_buffer_bytes);
}
