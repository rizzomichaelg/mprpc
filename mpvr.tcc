// -*- mode: c++ -*-
#include "clp.h"
#include "mpfd.hh"
#include "mpvr.hh"
#include <netdb.h>
#include <fcntl.h>
#include <unistd.h>

enum {
    m_vrc_go = 1,
    m_vrc_hello,
    m_vri_hello,
    m_vri_prepare,
    m_vri_commit
};

String Vrendpoint::make_uid() {
    FILE* f = fopen("/dev/urandom", "rb");
    uint64_t x = (uint64_t) (tamer::dnow() * 1000000);
    fread(&x, sizeof(x), 1, f);
    fclose(f);
    return String((char*) &x, 6).encode_base64();
}


// Login protocol.
//   message m_vri_hello:
//     request:  { group: GROUPNAME, uid: UID }
//     response: { ok: true,
//                 members: [ {addr: ADDR, port: PORT, uid: UID}... ],
//                 me: INDEX, master: INDEX }

bool Vrendpoint::connected() const {
    return true;
}

void Vrendpoint::connect(event<bool> done) {
    done(true);
}

void Vrendpoint::read_request(event<Json> done) {
    done(Json());
}

void Vrendpoint::call(Json, event<Json> done) {
    done(Json());
}


class Vrremote : public Vrendpoint {
  public:
    Vrremote(const String& uid, tamer::fd fd, Vrgroup* g);

    bool connected() const;
    tamed void connect(event<bool> done);

    tamed void read_request(event<Json> done);
    tamed void call(Json msg, event<Json> done);

  private:
    struct in_addr addr_;
    int port_;
    msgpack_fd mpfd_;
    Vrgroup* vrg_;
};

Vrremote::Vrremote(const String& uid, tamer::fd fd, Vrgroup* g)
    : Vrendpoint(uid), mpfd_(fd), vrg_(g) {
    // Read local socket name
    union {
        struct sockaddr sa;
        struct sockaddr_in in;
        struct sockaddr_storage s;
    } sa;
    socklen_t salen = sizeof(sa);
    int r = getsockname(fd.value(), &sa.sa, &salen);
    assert(r == 0);
    assert(sa.s.ss_family == AF_INET);
    addr_ = sa.in.sin_addr;
    port_ = ntohs(sa.in.sin_port);
}

bool Vrremote::connected() const {
    return mpfd_;
}

tamed void Vrremote::connect(event<bool> done) {
    tamed { int tries; tamer::fd cfd; Json j; }
    for (tries = 0; tries != 3 && mpfd_; ++tries)
        twait {
            ++connection_version_;
            tamer::tcp_connect(addr_, port_, vrg_->timeout(make_event(cfd)));
        }
    if (cfd) {
        mpfd_.clear();
        mpfd_.initialize(cfd);
    }
    twait {
        mpfd_.call(Json::array(m_vri_hello, Json::null, vrg_->group_name(), vrg_->uid(),
                               vrg_->view_number()),
                   tamer::with_timeout(connect_tj);
    }
    done(!!mpfd_);
}

void Vrremote::read_request(event<Json> done) {
    if (!mpfd_)
        twait { connect(tamer::unbind<bool>(make_event())); }
    if (mpfd_)
        mpfd_.read_request(done);
    else
        done(Json());
}

void Vrremote::call(Json msg, event<Json> done) {
    if (!mpfd_)
        twait { connect(tamer::unbind<bool>(make_event())); }
    if (mpfd_)
        mpfd_.call(msg, done);
    else
        done(Json());
}


#if 0
Vrclient::Vrclient(String group_name, tamer::fd listenfd)
    : group_name_(group_name), listenfd_(listenfd),
      viewno_(0), commitno_(0) {
    // Read 8 bytes from /dev/random to create a "unique" ID
    int f = open("/dev/random", O_RDONLY);
    assert(f >= 0);
    ssize_t nr = read(f, &uid_, sizeof(uid_));
    assert(nr == (ssize_t) sizeof(uid_));
    close(f);

    // Read local socket name
    union {
        struct sockaddr sa;
        struct sockaddr_in in;
        struct sockaddr_storage s;
    } sa;
    socklen_t salen = sizeof(sa);
    int r = getsockname(listenfd.value(), &sa.sa, &salen);
    assert(r == 0);
    assert(sa.s.ss_family == AF_INET);
    listenport_ = ntohs(sa.in.sin_port);

    network_.push_back(clientport(uid_, sa.in.sin_addr, listenport_));
    masterindex_ = index_ = 0;
}



// prepare: v#, op#, ...

tamed void Vrclient::replica() {
    tamed { Json j; }
    while (myindex_ != masterindex_) {
        j.clear();
        twait {
            network_[masterindex_].mpfd.read_request(j, tamer::with_timeout(timeout_, make_event(j)));
        }
        if (j.empty())
            break;
        if (j[0] == m_vri_prepare
            && j[2]
            view_change();
            
            
    }
}

void Vrclient::set_network(const Json& j) {
}

tamed void Vrclient::listener() {
    tvars { tamer::fd cfd; }
    while (listenfd_) {
        twait { listenfd_.accept(cfd); }
        if (cfd)
            handshake(cfd);
    }
}

tamed void Vrclient::handshake(tamer::fd cfd) {
    tvars { msgpack_fd* mpfd = new msgpack_fd(cfd); Json j; }
    twait { mpfd->read_request(j); }
    if (j && j.is_a() && j[0] == m_vrc_hello) {
        mpfd->write_reply(Json::array(-m_vrc_hello, j[1]));
        run_client(mpfd);
    } else if (j && j.is_a() && j[0] == m_vri_hello) {
        mpfd->write_reply(Json::array(-m_vri_hello, j[1]));
        run_interconnect(mpfd);
    } else
        delete mpfd;
}

tamed void Vrclient::run_interconnect(msgpack_fd* mpfd) {
    
}
#endif


int main(int argc, char** argv) {
    tamer::initialize();

    Vrclient vrc(tamer::tcp_listen(19218));

    tamer::loop();
    tamer::cleanup();
}
