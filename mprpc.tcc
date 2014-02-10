// -*- mode: c++ -*-
#include "clp.h"
#include "mpfd.hh"
#include <netdb.h>

static bool quiet = false;
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
        Json req, res = Json::make_array();
    }

    while (cfd) {
        twait { mpfd.read_request(make_event(req)); }
        if (!req || !req.is_a() || req.size() < 2 || !req[0].is_i()) {
            if (req)
                std::cerr << "bad RPC: " << req << std::endl;
            break;
        }

        res[0] = -req[0].as_i();
        res[1] = req[1];
        mpfd.write(res);
    }

    cfd.close();
}


tamed void client(const char* hostname, int port) {
    tvars {
        tamer::fd cfd;
        msgpack_fd mpfd;
        struct in_addr hostip;
        int i;
        Json req, res;
    }

    // lookup hostname address
    {
        in_addr_t a = hostname ? inet_addr(hostname) : htonl(INADDR_LOOPBACK);
        if (a != INADDR_NONE)
            hostip.s_addr = a;
        else {
            struct hostent* hp = gethostbyname(hostname);
            if (hp == NULL || hp->h_length != 4 || hp->h_addrtype != AF_INET) {
                std::cerr << "lookup " << hostname << ": " << hstrerror(h_errno) << std::endl;
                return;
            }
            hostip = *((struct in_addr*) hp->h_addr);
        }
    }

    // connect
    twait { tamer::tcp_connect(hostip, port, make_event(cfd)); }
    if (!cfd) {
        std::cerr << "connect " << (hostname ? hostname : "localhost")
                  << ":" << port << ": " << strerror(-cfd.error()) << std::endl;
        return;
    }
    mpfd.initialize(cfd);

    // pingpong 10 times
    for (i = 0; i != 10 && cfd; ++i) {
        req = Json::array(1, i);
        twait { mpfd.call(req, make_event(res)); }
        if (!quiet)
            std::cout << "call " << req << ": " << res << std::endl;
    }

    // close out
    cfd.close();
}


static Clp_Option options[] = {
    { "client", 'c', 0, 0, 0 },
    { "listen", 'l', 0, 0, 0 },
    { "port", 'p', 0, Clp_ValInt, 0 },
    { "host", 'h', 0, Clp_ValString, 0 },
    { "quiet", 'q', 0, 0, Clp_Negate }
};

int main(int argc, char** argv) {
    tamer::initialize();

    bool is_server = false;
    String hostname = "localhost";
    int port = 18029;
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
    }

    if (is_server)
        server(port);
    else
        client(hostname.c_str(), port);

    tamer::loop();
    tamer::cleanup();
}
