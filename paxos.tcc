// -*- mode: c++ -*-
#include "clp.h"
#include "mpfd.hh"
#include <netdb.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/wait.h>

#define WAIT            0   // waiting to start
#define PREPARE         1   // prepare message
#define PREPARED        2   // prepared message
#define ACCEPT          3   // accept message
#define ACCEPTED        4   // accepted message
#define DECIDED         5   // decided message

static bool quiet = false;

static inline double timestamp(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec + tv.tv_usec / 1000000.0;
}

tamed void client_init(const char* hostname, int port, tamer::fd& cfd, 
                        msgpack_fd& mpfd, struct in_addr& hostip,tamer::event<> done) {
    tvars {
        int s = 10000;
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

    // connect with exponential backoff
    twait { tamer::tcp_connect(hostip, port, make_event(cfd)); }
    while (!cfd) {
        usleep(s);
        twait { tamer::tcp_connect(hostip, port, make_event(cfd)); }
        if (s < 128000000)
            s *= 2;
        else
            break;
    }
    if (!cfd) {
        std::cerr << "connect " << (hostname ? hostname : "localhost")
                  << ":" << port << ": " << strerror(-cfd.error()) << std::endl;
        return;
    }
    mpfd.initialize(cfd);
    done();
}

tamed void run_acceptor(tamer::fd cfd,int& nconns) {
    tvars {
        msgpack_fd mpfd(cfd);
        Json req, res = Json::make_array();
        int decided = 0;
        int n_l = 0;
        int n_a = 0;
        int v_a = 0;
        int n,v;
    }

    while (cfd) {

        twait { mpfd.read_request(make_event(req)); }
        if (!req || !req.is_a() || req.size() < 3 || !req[0].is_i()) {
            if (req)
                std::cerr << "bad RPC: " << req << std::endl;
            break;
        }
        switch (req[2].as_i()) {
            case PREPARE:   
                // receive prepare
                assert(req.size() == 3);
                n = req[1].as_i();
                n_l = std::max(n_l,n);
                res[0] = -req[0].as_i();
                res[1] = req[1];
                res[2] = PREPARED;
                res[3] = n_a;
                res[4] = v_a;
                mpfd.write(res);
                break;
            case ACCEPT:
                //receive accept
                assert(req.size() == 4);
                n = req[1].as_i();
                v = req[3].as_i();
                if (n >= n_l) {
                    n_l = n_a = n;
                    v_a = v;
                }
                res[0] = -req[0].as_i();
                res[1] = req[1];
                res[2] = ACCEPTED;
                res[3] = n_a;
                mpfd.write(res);
                break;
            case DECIDED:
                if (!quiet)
                    std::cout << "Decided: " << req[3].as_i() << std::endl;
                res[0] = - req[0].as_i();
                res[1] = req[1];
                mpfd.write(req);
                goto done;
            default:
                std::cerr << "bad Paxos request: " << req << std::endl;
                break;
        }
    }
done: 
    --nconns;

    cfd.close();
}

tamed void run_proposer(const char* hostname, int port, int f);     

tamed void acceptor_init(int port,tamer::event<> done) {
    tvars {
        tamer::fd sfd = tamer::tcp_listen(port);
        tamer::fd cfd;
        int nconns;
    }
    if (sfd) {
        if (!quiet)
            std::cerr << "listening on port " << port << std::endl;
    } else
        std::cerr << "listen: " << strerror(-sfd.error()) << std::endl;
    while (sfd) {
        twait { 
            sfd.accept(make_event(cfd));
            run_acceptor(cfd,++nconns);
        }
        if (!quiet)
            std::cout << nconns << " connections" << std::endl;
    }
    done();
}

tamed void run_proposer(const char* hostname, std::vector<int> ports, int f,tamer::event<double> done) {
    tvars {
        std::vector<tamer::fd> cfd(ports.size());
        std::vector<msgpack_fd> mpfd(ports.size());
        struct in_addr hostip;
        std::vector<int>::size_type i;
        std::vector<Json> res(ports.size());
        Json req;
        int decided = 0;
        int n_p = 0;
        int n_o, v_o, n, v, a;
        double start,end;
    }

    for (i = 0; i < ports.size(); ++i)
        twait { client_init(hostname,ports[i],cfd[i],mpfd[i],hostip,make_event()); }

    start = timestamp();
start:
    // propose
    n_p++;
    n_o = 0;
    a = 0;
    req = Json::array(1,n_p,PREPARE);
    twait {
        for (i = 0; i < ports.size(); ++i)
            mpfd[i].call(req,make_event(res[i]));
    }
    // prepared
    for (i = 0 ; i < ports.size(); ++i) {
        if (res[i][2].as_i() != PREPARED)
            continue;
        n = res[i][3].as_i();
        v = res[i][4].as_i();
        if (n > n_o) {
            n_o = n;
            v_o = v;
        }
        a++;
    }
    if (a != f + 1)
        goto start;
    if (!quiet)
        std::cout << "prepared" << std::endl;
    if (v_o == 0)
        v_o = rand();
    a = 0;
    n_p = std::max(n_o,n_p);
    // send accept
    req = Json::array(1,n_p,ACCEPT,v_o);
    twait {
        for (i = 0; i < ports.size(); ++i)
            mpfd[i].call(req,make_event(res[i]));
    }
    // receive accepted
    for (i = 0; i < ports.size(); ++i) {
        n = res[i][3].as_i();
        v = res[i][4].as_i();
        if (n == n_p && res[i][2].as_i() == ACCEPTED)
            a++;
    }
    if (a != f + 1)
        goto start;
    if (!quiet)
        std::cout << "accepted" << std::endl;
    // send decided
    req = Json::array(1,n_p,DECIDED,v_o);
    twait {
        for (i = 0; i < ports.size(); ++i)
            mpfd[i].call(req,make_event(res[i]));
    }
    if (!quiet)
        std::cout << "decided" << std::endl;
    end = timestamp();

    // clean up
    for (i = 0; i < ports.size(); ++i)
        cfd[i].close();
    cfd.clear();
    mpfd.clear();
    if (done.result_pointer())
        *done.result_pointer() = end - start;
    done.unblock();
}

tamed void run_paxos(const char* hostname, std::vector<int> ports, int f, int n) {
    tvars { 
        int j,pid,status;
        std::vector<int>::size_type i;
        std::vector<int> children;
        Json res = Json::array();
        double ret;     
    }

    for (i = 0; i < ports.size(); ++i) {
        if ((pid = fork()) < 0) {
            perror("fork");
            exit(1);
        } else if (pid  == 0) {
            twait { acceptor_init(ports[i], make_event()); }
            exit(0);
        }
        children.push_back(pid);
    }
    
    for (j = 0; j < n; ++j) {
        twait { run_proposer(hostname,ports,f,make_event(ret)); }
        res.push_back(ret);
    }
    if (!quiet)
        std::cout << "Time: " << res << std::endl;
    
    // kill children
    for (i = 0; i < children.size(); ++i)
        kill(children[i],SIGTERM);
    children.clear();

    FILE* output = fopen("paxos.json","w");
    fprintf(output, "%s\n", res.unparse().c_str());
    fclose(output);
}


static Clp_Option options[] = {
    { "proc", 'f', 0, Clp_ValInt, 0 },
    { "instances", 'n', 0, Clp_ValInt, 0 },
    { "quiet", 'q', 0, 0, Clp_Negate }
};

int main(int argc, char** argv) {
    tamer::initialize();

    String hostname = "localhost";
    int port = 18029;
    int f = 1;
    int n = 1;
    Clp_Parser* clp = 
        Clp_NewParser(argc, argv, 
                      sizeof(options) / sizeof(options[0]), 
                      options);

    while (Clp_Next(clp) != Clp_Done) {
        if (Clp_IsLong(clp, "proc"))
            f = clp->val.i;
        else if (Clp_IsLong(clp, "instances"))
            n = clp->val.i;
        else if (Clp_IsLong(clp, "quiet"))
            quiet = !clp->negated;
    }
    int t = 2 * f + 1;
    if (t > 1)
        --t;
    std::vector<int> ports(t);
    for (std::vector<int>::size_type i = 0; i < ports.size(); ++i)
        ports[i] = port + i;

    run_paxos(hostname.c_str(),ports,f,n);

    tamer::loop();
    tamer::cleanup();
}
