// -*- mode: c++ -*-
#include "clp.h"
#include "mpfd.hh"
#include <netdb.h>

static bool quiet = false;
static bool stdout_isatty;
static Json param = Json();

tamed void handle_client(tamer::fd cfd);

enum {
    cmd_ping = 1,               // sends back input
    cmd_barrier = 2             // barrier_name, nproc[, args]
};


// Server code

struct barrier {
    String name_;
    size_t size_;
    std::vector<std::pair<msgpack_fd*, int> > clients_;
    Json response_;

    barrier(const String& name)
        : name_(name), response_(Json::array(-cmd_barrier, 0)) {
    }
    void add(msgpack_fd* mpfd, int seqno, size_t mysize, Json myresponse) {
        if (clients_.empty())
            size_ = mysize;
        assert(size_ == mysize);
        clients_.push_back(std::make_pair(mpfd, seqno));
        response_.push_back(myresponse);
        if (clients_.size() == mysize)
            release();
    }
    void remove(msgpack_fd* mpfd) {
        for (auto it = clients_.begin(); it != clients_.end(); ++it)
            if (it->first == mpfd)
                it->first = 0;
    }
    void release() {
        for (auto it = clients_.begin(); it != clients_.end(); ++it)
            if (it->first) {
                response_[1] = it->second;
                it->first->write(response_);
            }
        clients_.clear();
    }
    bool done() const {
        return clients_.empty();
    }
};
std::vector<barrier> barriers;

tamed void server(int port) {
    tvars {
        tamer::fd sfd = tamer::tcp_listen(port);
        tamer::fd cfd;
    }
    if (sfd)
        std::cerr << "listening on port " << port << std::endl;
    else
        std::cerr << "listen: " << strerror(-sfd.error()) << std::endl;
    while (sfd) {
        twait { sfd.accept(make_event(cfd)); }
        handle_client(cfd);
    }
}

tamed void handle_client(tamer::fd cfd) {
    tvars {
        msgpack_fd mpfd(cfd);
        Json req;
    }

    while (cfd) {
        twait { mpfd.read_request(make_event(req)); }
        if (!req || !req.is_a() || req.size() < 2 || !req[0].is_i()) {
            if (req)
                std::cerr << "bad RPC: " << req << std::endl;
            break;
        }

        int cmd = req[0].as_i();
        req[0] = -cmd;

        if (cmd == cmd_ping)
            /* do nothing */;
        else if (cmd == cmd_barrier
                 && req.size() >= 4
                 && req[2].is_s()
                 && req[3].is_i()) {
            String bname = req[2].as_s();
            size_t bno = 0;
            while (bno != barriers.size() && barriers[bno].name_ != bname)
                ++bno;
            if (bno == barriers.size())
                barriers.push_back(barrier(bname));

            barriers[bno].add(&mpfd, req[1].as_i(), req[3].as_i(), req[4]);
            if (barriers[bno].done())
                barriers.erase(barriers.begin() + bno);
            continue;
        } else
            req.resize(2);

        mpfd.write(req);
    }

    cfd.close();
    for (size_t bno = 0; bno != barriers.size(); ++bno)
        barriers[bno].remove(&mpfd);
}


// Client code

tamed void client_connect(const char* hostname, int port,
                          tamer::event<tamer::fd> done) {
    tvars {
        tamer::fd fd;
        struct in_addr hostip;
    }

    // look up hostname as dotted quad
    hostip.s_addr = hostname ? inet_addr(hostname) : htonl(INADDR_LOOPBACK);
    // on failure, look up hostname using DNS (blocking)
    if (hostip.s_addr == INADDR_NONE) {
        struct hostent* hp = gethostbyname(hostname);
        if (hp == NULL || hp->h_length != 4 || hp->h_addrtype != AF_INET) {
            std::cerr << "lookup " << hostname << ": " << hstrerror(h_errno) << std::endl;
            done(tamer::fd(-EBADF));
            return;
        }
        hostip = *((struct in_addr*) hp->h_addr);
    }

    // connect
    twait { tamer::tcp_connect(hostip, port, make_event(fd)); }

    // complain on failure
    if (!fd)
        std::cerr << "connect " << (hostname ? hostname : "localhost") << ":" << port
                  << ": " << strerror(-fd.error()) << std::endl;
    done(fd);
}


tamed void client_pingpong(tamer::fd cfd, size_t n, Json req) {
    tvars {
        msgpack_fd mpfd(cfd);
        size_t i;
        Json res;
    }

    for (i = 0; i != n && mpfd; ++i) {
        twait { mpfd.call(req, make_event(res)); }
        if (!quiet)
            std::cout << "call " << req << ": " << res << std::endl;
    }
}

tamed void clientf_pingpong(const char* hostname, int port, Json req,
                            int ignore1, int ignore2) {
    tvars { tamer::fd cfd; }
    twait { client_connect(hostname, port, make_event(cfd)); }
    client_pingpong(cfd, param["count"].as_u(10), req);
}


static void print_report(size_t nsent, size_t nrecv,
                         double sent_mb, double recv_mb,
                         double deltat, bool last) {
    static int last_n = 0;
    if (last || (stdout_isatty && !quiet)) {
        if (last_n)
            fprintf(stdout, "\r%.*s\r", last_n, "");
        last_n = fprintf(stdout, "%.3fs: %zu (%.3fMB) sent, %zu (%.3fMB) recv, %.3f/s (%.3fMB/s)%s",
                         deltat, nsent, sent_mb, nrecv, recv_mb,
                         (nsent + nrecv) / deltat,
                         (sent_mb + recv_mb) / deltat,
                         last ? "\n" : "");
        fflush(stdout);
        if (last)
            last_n = 0;
    }
}

static void print_report(size_t nsent, size_t nrecv, const msgpack_fd& mpfd,
                         double deltat, bool last) {
    print_report(nsent, nrecv,
                 mpfd.sent_bytes() / 1000000., mpfd.recv_bytes() / 1000000.,
                 deltat, last);
}

tamed void client_windowed(msgpack_fd& mpfd, size_t n, size_t w, Json req,
                           int clientno, tamer::event<Json> done) {
    tvars {
        size_t i, out = 0, sz, bthresh = 1 << 24, nthresh = 1 << 16;
        double t0, t1, tthresh;
        tamer::rendezvous<> r;
        Json* res = new Json[w];
    }

    t0 = tamer::dnow();
    tthresh = t0 + 0.5;
    for (i = 0; (i != n || out) && mpfd; ) {
        if (i != n && out < w) {
            mpfd.call(req, r.make_event(res[i % w]));
            ++out;
            ++i;
            if ((i % (1 << 12)) == 0)
                tamer::set_recent();
        } else {
            twait(r);
            --out;
        }
        if (clientno == 0 && !quiet
            && (i % (1 << 12)) == 0
            && tamer::drecent() >= tthresh) {
            print_report(i, i - out, mpfd, tamer::dnow() - t0, false);
            tthresh += 0.5;
        }
    }

    done(Json().set("time", tamer::dnow() - t0)
         .set("nsent", i).set("sent_mb", mpfd.sent_bytes() / 1000000.)
         .set("nrecv", i - out).set("recv_mb", mpfd.recv_bytes() / 1000000.));

    delete[] res;
}

tamed void clientf_windowed(const char* hostname, int port, Json req,
                            int clientno, int nclients) {
    tvars { tamer::fd cfd; msgpack_fd mpfd; Json j; }
    twait { client_connect(hostname, port, make_event(cfd)); }
    mpfd.initialize(cfd);
    twait { mpfd.call(Json::array(cmd_barrier, 1, "start", nclients),
                      make_event(j)); }
    twait { client_windowed(mpfd, param["count"].as_u(1000000),
                            param["window"].as_u(10), req,
                            clientno, make_event(j)); }
    twait { mpfd.call(Json::array(cmd_barrier, mpfd.call_seq(), "end",
                                  nclients, j),
                      make_event(j)); }

    if (clientno == 0) {
        size_t nsent = 0, nrecv = 0;
        double sent_mb = 0, recv_mb = 0, deltat = 0;
        for (int i = 0; i != nclients; ++i) {
            nsent += j[2 + i]["nsent"].as_u();
            nrecv += j[2 + i]["nrecv"].as_u();
            sent_mb += j[2 + i]["sent_mb"].as_d();
            recv_mb += j[2 + i]["recv_mb"].as_d();
            if (j[2 + i]["time"].as_d() > deltat)
                deltat = j[2 + i]["time"].as_d();
        }
        print_report(nsent, nrecv, sent_mb, recv_mb, deltat, true);
    }
}


static Clp_Option options[] = {
    { "client", 'c', 0, 0, 0 },
    { "listen", 'l', 0, 0, 0 },
    { "port", 'p', 0, Clp_ValInt, 0 },
    { "host", 'h', 0, Clp_ValString, 0 },
    { "quiet", 'q', 0, 0, Clp_Negate },
    { "window", 'w', 0, Clp_ValUnsignedLong, 0 },
    { "count", 'n', 0, Clp_ValUnsignedLong, 0 },
    { "datasize", 'd', 0, Clp_ValUnsignedLong, Clp_Negate },
    { "nclients", 'j', 0, Clp_ValInt, 0 },
};

int main(int argc, char** argv) {
    tamer::initialize();

    bool is_server = false;
    void (*clientf)(const char*, int, Json, int, int) = clientf_windowed;
    Json req_prototype = Json::array(cmd_ping, Json::null);
    String hostname = "localhost";
    int port = 18029;
    stdout_isatty = isatty(STDOUT_FILENO);
    Clp_Parser* clp = Clp_NewParser(argc, argv, sizeof(options) / sizeof(options[0]), options);
    param.set("nclients", 1);

    while (Clp_Next(clp) != Clp_Done) {
        if (Clp_IsLong(clp, "listen"))
            is_server = true;
        else if (Clp_IsLong(clp, "client"))
            is_server = false;
        else if (Clp_IsLong(clp, "nclients"))
            param.set("nclients", clp->val.i);
        else if (Clp_IsLong(clp, "port"))
            port = clp->val.i;
        else if (Clp_IsLong(clp, "host"))
            hostname = clp->vstr;
        else if (Clp_IsLong(clp, "quiet"))
            quiet = !clp->negated;
        else if (Clp_IsLong(clp, "window"))
            param.set("window", clp->val.ul);
        else if (Clp_IsLong(clp, "count"))
            param.set("count", clp->val.ul);
        else if (Clp_IsLong(clp, "datasize")) {
            if (clp->negated || clp->val.ul == 0)
                req_prototype.resize(2);
            else
                req_prototype[2] = String::make_fill('x', clp->val.ul);
        }
    }

    if (is_server)
        server(port);
    else {
        int nclients = param["nclients"].to_i();
        int clientno = nclients - 1;
        while (clientno > 0 && fork() != 0)
            --clientno;
        clientf(hostname.c_str(), port, req_prototype, clientno, nclients);
    }

    tamer::loop();
    tamer::cleanup();
}
