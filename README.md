Tema realizata de: Vlad Părău
Grupa: 322CC

---

### protocol.h

S-au adaugat definitiile tipurilor de segmente folosite in protocol:

* TYPE_SYN (0x01) — primul pachet trimis de client pentru a initia o conexiune.
* TYPE_SYN_ACK (0x02) — raspunsul serverului; contine in payload portul dedicat pe care clientul trebuie sa trimita datele.
* TYPE_ACK (0x03) — confirmarea clientului care incheie handshake-ul three-way.
* TYPE_DATA (0x04) — segmentele cu date utile trimise de sender la receiver.
* TYPE_CTRL (0x05) — pachetele de ACK trimise de receiver inapoi la sender in timpul transferului.
* SERVER_PORT (8032) — portul fix pe care serverul asculta cereri de conexiune.

poli_tcp_synack — headerul de control urmat de data_port, reprezentand portul dedicat trimis de server in cadrul handshake-ului.

---

### lib.h

**Campuri adaugate pentru sender:**

* max_window_seq — numarul maxim de segmente care pot fi trimise dar neconfirmate simultan; calculat pe baza BDP (Bandwidth-Delay Product).
* recv_window_peer — fereastra de receptie anuntata de receiver in ultimul ACK primit; folosita pentru flow control.
* send_base — numarul de secventa al celui mai vechi segment trimis neconfirmat; reprezinta inceputul ferestrei glisante.
* next_seq — numarul de secventa al urmatorului segment de trimis.
* send_buf — coada in care send_data pune bytes veniti de la aplicatie; sender_handler ii scade si ii impacheteaza in segmente.
* window_segs — copii ale segmentelor trimise dar neconfirmate, indexate dupa numarul de secventa; folosite pentru retransmisie la timeout.

**Campuri adaugate pentru receiver:**

* expected_seq — urmatorul numar de secventa asteptat in ordine; tot ce vine inainte de aceasta valoare a fost deja livrat aplicatiei.
* recv_buf — coada cu bytes receptionati in ordine, gata sa fie livrati prin recv_data.
* recv_window — segmente sosite out-of-order, stocate temporar pana cand golul din secventa se umple (Selective Repeat).
* data_ready — variabila de conditie pe care recv_data asteapta atunci cand recv_buf este gol; receiver_handler o semnalizeaza cand adauga date noi.

### libsend.cpp — clasa Sender

* **init_sender()** — salveaza parametrii legaturii si porneste thread-ul sender_handler.
* **setup_connection()** — executa three-way handshake cu retry, calculeaza fereastra din BDP si inregistreaza conexiunea; returneaza conn_id.
* **send_data()** — copiaza datele in send_buf sub mutex; nu trimite UDP direct.
* **pump_window()** — scoate date din send_buf si trimite segmente cat timp fereastra permite; pastreaza copii in window_segs pentru retransmisie.
* **sender_handler()** — asteapta ACK-uri si timeouturi; la timeout retransmite window_segs, la ACK avanseaza send_base; la fiecare iteratie apeleaza pump_window pentru toate conexiunile.

### librecv.cpp — clasa Receiver

* **init_receiver()** — creeaza socket-ul de listening pe portul 8032 si porneste thread-ul receiver_handler.
* **wait4connect()** — executa three-way handshake pe server cu retry; foloseste MSG_PEEK pentru cazul in care ACK-ul se pierde; returneaza conn_id.
* **recv_data()** — copiaza date din recv_buf in bufferul aplicatiei; se blocheaza pe pthread_cond_wait daca nu sunt date disponibile.
* **receiver_handler()** — primeste segmente, le stocheaza in recv_window, le livreaza in ordine in recv_buf, semnalizeaza data_ready si trimite ACK.

---

## Protocolul dezvoltat

Protocolul este un mecanism de transport fiabil peste UDP, similar cu TCP, construit din trei componente principale.

**Three-way handshake.** Inainte de a transfera date, clientul si serverul stabilesc o conexiune in trei pasi: clientul trimite SYN, serverul raspunde cu SYN-ACK (care contine si portul dedicat pe care va asculta datele), iar clientul confirma cu ACK. Dupa handshake, toata comunicarea de date are loc pe portul dedicat deschis de server, nu pe portul 8032. Ambele parti retransmit la 200ms.

**Fereastra glisanta cu Selective Repeat ARQ.** Senderul poate trimite mai multe segmente fara a astepta confirmare pentru fiecare, pana la limita ferestrei. Fereastra este calculata din BDP (Bandwidth-Delay Product): latimea de banda inmultita cu RTT-ul, impartita la dimensiunea unui segment. La expirarea unui timer, retransmite toate segmentele neconfirmate din fereastra. Receiverele accepta segmente sosite out-of-order si le reordoneaza inainte de a le livra aplicatiei, trimitand ACK dupa fiecare segment primit.

**Flow control.** Receiverele anunta in fiecare ACK cate segmente mai pot accepta (campul recv_window). Senderul nu depaseste aceasta valoare, evitand astfel depasirea capacitatii bufferului de receptie.

Numerele de secventa sunt pe 16 biti (0–65535) cu overflow controlat la % 65536.