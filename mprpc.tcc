// -*- mode: c++ -*-
#include "clp.h"
#include "mpfd.hh"
#include <netdb.h>

static bool quiet = false;
static bool stdout_isatty;
static Json param = Json();

tamed void handle_client(tamer::fd cfd);


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

        req[0] = -req[0].as_i();
        mpfd.write(req);
    }

    cfd.close();
}


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
        req[1] = i;
        twait { mpfd.call(req, make_event(res)); }
        if (!quiet)
            std::cout << "call " << req << ": " << res << std::endl;
    }
}

tamed void clientf_pingpong(const char* hostname, int port, Json req) {
    tvars { tamer::fd cfd; }
    twait { client_connect(hostname, port, make_event(cfd)); }
    client_pingpong(cfd, param["count"].as_u(10), req);
}


static void print_report(const msgpack_fd& mpfd, size_t nsent, size_t nrecv, double deltat, bool last) {
    static int last_n = 0;
    if (last || (stdout_isatty && !quiet)) {
        if (last_n)
            fprintf(stdout, "\r%.*s\r", last_n, "");
        last_n = fprintf(stdout, "%.3fs: %zu (%.3fMB) sent, %zu (%.3fMB) recv, %.3fMB/s tput%s",
                         deltat, nsent, mpfd.sent_bytes() / 1000000.,
                         nrecv, mpfd.recv_bytes() / 1000000.,
                         (mpfd.sent_bytes() + mpfd.recv_bytes()) / (1000000. * deltat),
                         last ? "\n" : "");
        fflush(stdout);
        if (last)
            last_n = 0;
    }
}

tamed void client_windowed(tamer::fd cfd, size_t n, size_t w, Json req) {
    tvars {
        msgpack_fd mpfd(cfd);
        size_t i, out = 0, sz, bthresh = 1 << 24, nthresh = 1 << 16;
        double t0, t1, tthresh;
        tamer::rendezvous<> r;
        Json* res = new Json[w];
    }

    t0 = tamer::dnow();
    tthresh = t0 + 0.5;
    for (i = 0; (i != n || out) && mpfd; ) {
        if (i != n && out < w) {
            req[1] = i;
            mpfd.call(req, r.make_event(res[i % w]));
            ++out;
            ++i;
            if ((i % (1 << 12)) == 0)
                tamer::set_now();
        } else {
            twait(r);
            --out;
        }
        if (!quiet
            && (i % (1 << 12)) == 0
            && tamer::dnow() >= tthresh) {
            print_report(mpfd, i, i - out, tamer::dnow() - t0, false);
            tthresh += 0.5;
        }
    }
    print_report(mpfd, i, i - out, tamer::dnow() - t0, true);

    delete[] res;
}

tamed void clientf_windowed(const char* hostname, int port, Json req) {
    tvars { tamer::fd cfd; }
    twait { client_connect(hostname, port, make_event(cfd)); }
    client_windowed(cfd, param["count"].as_u(1000000), param["window"].as_u(10), req);
}


static Clp_Option options[] = {
    { "client", 'c', 0, 0, 0 },
    { "listen", 'l', 0, 0, 0 },
    { "port", 'p', 0, Clp_ValInt, 0 },
    { "host", 'h', 0, Clp_ValString, 0 },
    { "quiet", 'q', 0, 0, Clp_Negate },
    { "window", 'w', 0, Clp_ValUnsignedLong, 0 },
    { "count", 'n', 0, Clp_ValUnsignedLong, 0 },
    { "datasize", 'd', 0, Clp_ValUnsignedLong, Clp_Negate }
};

int main(int argc, char** argv) {
    tamer::initialize();

    bool is_server = false;
    void (*clientf)(const char*, int, Json) = clientf_windowed;
    Json req_prototype = Json::array(1, 1);
    String hostname = "localhost";
    int port = 18029;
    stdout_isatty = isatty(STDOUT_FILENO);
    Clp_Parser* clp = Clp_NewParser(argc, argv, sizeof(options) / sizeof(options[0]), options);

    while (Clp_Next(clp) != Clp_Done) {
        if (Clp_IsLong(clp, "listen"))
            is_server = true;
        else if (Clp_IsLong(clp, "client"))
            is_server = false;
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
    else
        clientf(hostname.c_str(), port, req_prototype);

    tamer::loop();
    tamer::cleanup();
}
