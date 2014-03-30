// -*- mode: c++ -*-
#include "clp.h"
#include "mpfd.hh"
#include "mpvr.hh"
#include <netdb.h>
#include <fcntl.h>
#include <unistd.h>
#include <algorithm>
#include <tamer/channel.hh>

enum {
    m_vri_view = 1,
    m_vri_error = 100
};

std::ostream& operator<<(std::ostream& out, const timeval& tv) {
    char buf[40];
    int x = sprintf(buf, "%ld.%06ld", (long) tv.tv_sec, (long) tv.tv_usec);
    out.write(buf, x);
    return out;
}

String Vrendpoint::make_uid() {
#if 0
    FILE* f = fopen("/dev/urandom", "rb");
    uint64_t x = (uint64_t) (tamer::dnow() * 1000000);
    size_t n = fread(&x, sizeof(x), 1, f);
    assert(n == 1);
    fclose(f);
    return String((char*) &x, 6).encode_base64();
#else
    static int counter;
    return String("n") + String(counter++);
#endif
}


// Login protocol.
//   message m_vri_hello:
//     request:  { group: GROUPNAME, uid: UID }
//     response: { ok: true,
//                 members: [ {addr: ADDR, port: PORT, uid: UID}... ],
//                 me: INDEX, primary: INDEX }

Vrgroup::view_type::view_type()
    : viewno(0), primary_index(0), my_index(-1), nacked(0), nconfirmed(0) {
}

Vrgroup::view_type Vrgroup::view_type::make_singular(Json peer_name) {
    view_type v;
    v.members.push_back(view_member(peer_name));
    v.primary_index = v.my_index = 0;
    return v;
}

bool Vrgroup::view_type::assign(Json msg, const String& my_uid) {
    if (!msg.is_o())
        return false;
    Json viewnoj = msg["viewno"];
    Json membersj = msg["members"];
    Json primaryj = msg["primary"];
    if (!(viewnoj.is_i() && viewnoj.to_i() >= 0
          && membersj.is_a()
          && primaryj.is_i()
          && primaryj.to_i() >= 0 && primaryj.to_i() < membersj.size()))
        return false;

    viewno = viewnoj.to_u64();
    primary_index = primaryj.to_i();
    my_index = -1;

    std::unordered_map<String, int> seen_uids;
    String uid;
    for (auto it = membersj.abegin(); it != membersj.aend(); ++it) {
        if (!it->is_object()
            || !it->get("uid").is_string()
            || !(uid = it->get("uid").to_s())
            || seen_uids.find(uid) != seen_uids.end())
            return false;
        seen_uids[uid] = 1;
        if (uid == my_uid)
            my_index = it - membersj.abegin();
        members.push_back(view_member(*it));
        /*else if (!it->get("addr").is_string()
          || !it->get("port").is_int()
          || it->get("port").to_i() <= 0
                 || it->get("port").to_i() > 65535)
                 return false;*/
    }

    return true;
}

inline int Vrgroup::view_type::count(const String& uid) const {
    for (auto it = members.begin(); it != members.end(); ++it)
        if (it->uid == uid)
            return 1;
    return 0;
}

inline Vrgroup::view_member* Vrgroup::view_type::find_pointer(const String& uid) {
    for (auto it = members.begin(); it != members.end(); ++it)
        if (it->uid == uid)
            return &*it;
    return nullptr;
}

inline String Vrgroup::view_type::primary_uid() const {
    return members[primary_index].uid;
}

inline Json Vrgroup::view_type::members_json() const {
    Json j = Json::array();
    for (auto it = members.begin(); it != members.end(); ++it) {
        j.push_back(it->peer_name);
        if (it->acked)
            j.back()["acked"] = true;
        if (it->confirmed)
            j.back()["confirmed"] = true;
    }
    return j;
}

bool Vrgroup::view_type::operator==(const view_type& x) const {
    if (viewno != x.viewno
        || primary_index != x.primary_index
        || my_index != x.my_index
        || members.size() != x.members.size())
        return false;
    for (size_t i = 0; i != members.size(); ++i)
        if (members[i].uid != x.members[i].uid)
            return false;
    return true;
}

bool Vrgroup::view_type::shared_quorum(const view_type& x) const {
    size_t nshared = 0;
    for (auto it = members.begin(); it != members.end(); ++it)
        if (x.count(it->uid))
            ++nshared;
    return nshared == size()
        || nshared == x.size()
        || (nshared > f() && nshared > x.f());
}

void Vrgroup::view_type::prepare(Vrendpoint* ep, const Json& payload) {
    auto it = members.begin();
    while (it != members.end() && it->uid != ep->uid())
        ++it;
    if (it != members.end()) {
        if (!it->acked) {
            it->acked = true;
            ++nacked;
        }
        if (payload["confirm"] && !it->confirmed) {
            it->confirmed = true;
            ++nconfirmed;
        }
    }
}

void Vrgroup::view_type::clear_preparation() {
    nacked = nconfirmed = 0;
    for (auto it = members.begin(); it != members.end(); ++it)
        it->acked = it->confirmed = false;
}

void Vrgroup::view_type::add(Json peer_name, const String& my_uid) {
    if (peer_name.is_s())
        peer_name = Json::object("uid", peer_name);
    String peer_uid = peer_name["uid"].to_s();
    auto it = members.begin();
    while (it != members.end() && it->uid < peer_uid)
        ++it;
    if (it == members.end() || it->uid != peer_uid)
        members.insert(it, view_member(peer_name));

    clear_preparation();
    ++viewno;
    if (!viewno)
        ++viewno;

    my_index = -1;
    for (size_t i = 0; i != members.size(); ++i)
        if (members[i].uid == my_uid)
            my_index = i;
    primary_index = viewno % members.size();
}


Vrgroup::Vrgroup(const String& group_name, Vrendpoint* me)
    : group_name_(group_name), want_member_(!!me), me_(me), commitno_(0) {
    if (me_) {
        cur_view_ = view_type::make_singular(me_->name());
        endpoints_[me->uid()] = me;
        listen_loop();
    }
    next_view_ = cur_view_;
}

void Vrgroup::dump(std::ostream& out) const {
    timeval now = tamer::now();
    out << now << ":" << uid() << ": " << unparse_view_state()
        << " " << cur_view_.members_json()
        << " p@" << cur_view_.primary_index << "\n";
}

String Vrgroup::unparse_view_state() const {
    StringAccum sa;
    sa << "v#" << cur_view_.viewno
       << (cur_view_.primary_index == cur_view_.my_index ? "p" : "");
    if (next_view_.viewno != cur_view_.viewno)
        sa << "<v#" << next_view_.viewno
           << (next_view_.primary_index == next_view_.my_index ? "p" : "")
           << ":" << next_view_.nacked << "." << next_view_.nconfirmed << ">";
    return sa.take_string();
}

tamed void Vrgroup::listen_loop() {
    tamed { Vrendpoint* peer; }
    while (1) {
        twait { me_->receive_connection(make_event(peer)); }
        if (!peer)
            break;
        if (endpoints_[peer->uid()])
            delete endpoints_[peer->uid()];
        endpoints_[peer->uid()] = peer;
        interconnect_loop(peer);
    }
}

tamed void Vrgroup::connect(Json peer_name, event<Vrendpoint*> done) {
    tamed { Vrendpoint* peer; }
    assert(me_);
    if (peer_name.is_s())
        peer_name = Json::object("uid", peer_name);
    twait { me_->connect(peer_name, make_event(peer)); }
    if (peer) {
        peer_name["uid"] = peer->uid();
        if (endpoints_[peer->uid()])
            delete endpoints_[peer->uid()];
        endpoints_[peer->uid()] = peer;
        interconnect_loop(peer);
        done(peer);
    } else
        done(nullptr);
}

tamed void Vrgroup::join(Json peer_name, event<Vrendpoint*> done) {
    tamed { Vrendpoint* peer; }
    assert(want_member_);
    twait { connect(peer_name, make_event(peer)); }
    if (peer)
        send_view(peer);
    // XXX trigger done only when joined
    done(peer);
}

tamed void Vrgroup::interconnect_loop(Vrendpoint* peer) {
    tamed { Json msg; }
    while (1) {
        twait { peer->receive(make_event(msg)); }
        std::cout << tamer::now() << ":" << uid() << " <- " << peer->uid()
                  << ": recv " << msg << " " << unparse_view_state() << "\n";
        if (!msg || !msg.is_a() || msg.size() < 2 || !msg[0].is_i())
            break;
        if (msg[0] == m_vri_view)
            process_view(peer, msg);
    }
}

Vrendpoint* Vrgroup::primary(const view_type& v) const {
    if ((unsigned) v.primary_index < (unsigned) v.members.size())
        return endpoints_[v.members[v.primary_index].uid];
    else
        return nullptr;
}

void Vrgroup::process_view(Vrendpoint* who, const Json& msg) {
    Json payload = msg[2];
    view_type v;
    if (!v.assign(payload, me_->uid())
        || !v.count(who->uid())) {
        who->send(Json::array((int) m_vri_error, -msg[1]));
        return;
    }

    viewnumberdiff_t vdiff = (viewnumberdiff_t) (v.viewno - next_view_.viewno);

    // view #0 is special and indicates an attempt to join the group
    bool v_changed = false;
    if (next_view_.viewno == 0 && v != next_view_) {
        vdiff = 1;
        if (!v.count(me_->uid())) {
            v.add(me_->uid(), me_->uid());
            v_changed = true;
        }
    }

    bool want_send = false;
    if (vdiff < 0
        || (vdiff == 0 && v != next_view_)
        || !next_view_.shared_quorum(v))
        want_send = true;
    else if (vdiff == 0 && cur_view_.viewno == next_view_.viewno)
        return;
    else if (vdiff == 0) {
        cur_view_.prepare(who, payload);
        next_view_.prepare(who, payload);
        if (payload["adopt"]) {
            next_view_.clear_preparation();
            cur_view_ = next_view_;
            return;
        }
        want_send = !payload["ack"] && !payload["confirm"];
    } else {
        // start new view
        cur_view_.clear_preparation();
        next_view_sent_confirm_ = false;
        next_view_ = v;
        cur_view_.prepare(me_, payload);
        next_view_.prepare(me_, payload);
        if (!v_changed) {
            cur_view_.prepare(who, payload);
            next_view_.prepare(who, payload);
        }
        broadcast_view();
    }

    if (cur_view_.nacked > cur_view_.f()
        && next_view_.nacked > next_view_.f()
        && (next_view_.primary_index == next_view_.my_index
            || next_view_.members[next_view_.primary_index].acked)
        && !next_view_sent_confirm_) {
        if (next_view_.primary_index != next_view_.my_index) {
            Vrendpoint* nextpri = primary(next_view_);
            send_view(nextpri);
            want_send = want_send && nextpri != who;
        } else
            next_view_.prepare(me_, Json::object("confirm", true));
        next_view_sent_confirm_ = true;
    }
    if (next_view_.nconfirmed > next_view_.f()
        && next_view_.my_index == next_view_.primary_index) {
        broadcast_view();
        next_view_.clear_preparation();
        cur_view_ = next_view_;
    } else if (want_send)
        send_view(who);
}

Json Vrgroup::view_payload(const String& peer_uid) {
    Json payload = Json::object("viewno", next_view_.viewno,
                                "members", next_view_.members_json(),
                                "primary", next_view_.primary_index);
    if (next_view_.viewno != cur_view_.viewno) {
        auto it = next_view_.members.begin();
        while (it != next_view_.members.end() && it->uid != peer_uid)
            ++it;
        if (it != next_view_.members.end() && it->acked)
            payload["ack"] = true;
        if (cur_view_.nacked > cur_view_.f()
            && next_view_.nacked > next_view_.f())
            payload["confirm"] = true;
        if (next_view_.nconfirmed > next_view_.f()
            && next_view_.my_index == next_view_.primary_index)
            payload["adopt"] = true;
    }
    return payload;
}

void Vrgroup::send_view(Vrendpoint* who, Json payload) {
    if (!payload.get("members"))
        payload.merge(view_payload(who->uid()));
    who->send(Json::array((int) m_vri_view, Json::null, payload));
    std::cout << tamer::now() << ":"
              << me_->uid() << " -> " << who->uid() << ": send " << payload
              << " " << unparse_view_state() << "\n";
}

tamed void Vrgroup::send_view(Json peer_name) {
    tamed {
        Json payload = view_payload(peer_name["uid"].to_s());
        Vrendpoint* ep;
    }
    if (!(ep = endpoints_[peer_name["uid"].to_s()]))
        twait { connect(peer_name, make_event(ep)); }
    if (ep && ep != me_)
        send_view(ep, payload);
}

void Vrgroup::broadcast_view() {
    for (auto it = next_view_.members.begin();
         it != next_view_.members.end(); ++it)
        send_view(it->peer_name);
}


// Vrendpoint

class Vrtestconnection;
class Vrtestlistener;

class Vrtestnode {
  public:
    inline Vrtestnode(const String& uid, std::vector<Vrtestnode*>& collection);

    inline const String& uid() const {
        return uid_;
    }
    inline Vrtestlistener* listener() const {
        return listener_;
    }

    Vrtestnode* find(const String& uid) const;

  private:
    String uid_;
    std::vector<Vrtestnode*>& collection_;
    Vrtestlistener* listener_;
};

class Vrtestlistener : public Vrendpoint {
  public:
    inline Vrtestlistener(Vrtestnode* my_node)
        : Vrendpoint(my_node->uid()), my_node_(my_node) {
    }
    void connect(Json def, event<Vrendpoint*> done);
    void receive_connection(event<Vrendpoint*> done);
  private:
    Vrtestnode* my_node_;
    tamer::channel<Vrendpoint*> listenq_;
};

class Vrtestconnection : public Vrendpoint {
  public:
    inline Vrtestconnection(Vrtestnode* from, Vrtestnode* to)
        : Vrendpoint(to->uid()), from_node_(from) {
    }
    ~Vrtestconnection();
    void send(Json msg);
    void receive(event<Json> done);
  private:
    Vrtestnode* from_node_;
    tamer::channel<Json> q_;
    Vrtestconnection* peer_;
    friend class Vrtestlistener;
};

Vrtestnode::Vrtestnode(const String& uid, std::vector<Vrtestnode*>& collection)
    : uid_(uid), collection_(collection) {
    collection_.push_back(this);
    listener_ = new Vrtestlistener(this);
}

Vrtestnode* Vrtestnode::find(const String& uid) const {
    for (auto x : collection_)
        if (x->uid() == uid)
            return x;
    return nullptr;
}

Vrtestconnection::~Vrtestconnection() {
    if (peer_) {
        peer_->peer_ = 0;
        while (!peer_->q_.wait_empty())
            peer_->q_.push_back(Json());
    }
}

void Vrtestconnection::send(Json msg) {
    if (peer_)
        peer_->q_.push_back(msg);
}

void Vrtestconnection::receive(event<Json> done) {
    if (peer_)
        q_.pop_front(done);
    else
        done(Json());
}

void Vrtestlistener::connect(Json def, event<Vrendpoint*> done) {
    if (Vrtestnode* n = my_node_->find(def["uid"].to_s())) {
        assert(n->uid() != uid());
        Vrtestconnection* my = new Vrtestconnection(my_node_, n);
        Vrtestconnection* peer = new Vrtestconnection(n, my_node_);
        my->peer_ = peer;
        peer->peer_ = my;
        n->listener()->listenq_.push_back(peer);
        done(my);
    } else
        done(nullptr);
}

void Vrtestlistener::receive_connection(event<Vrendpoint*> done) {
    listenq_.pop_front(done);
}


Json Vrendpoint::name() const {
    return Json::object("uid", uid());
}

void Vrendpoint::connect(Json, event<Vrendpoint*>) {
    assert(0);
}

void Vrendpoint::receive_connection(event<Vrendpoint*>) {
    assert(0);
}

void Vrendpoint::send(Json) {
    assert(0);
}

void Vrendpoint::receive(event<Json>) {
    assert(0);
}


#if 0
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
                   vrg_->timeout(make_event(j)));
    }
    done(!!mpfd_);
}

tamed void Vrremote::read_request(event<Json> done) {
    if (!mpfd_)
        twait { connect(tamer::rebind<bool>(make_event())); }
    if (mpfd_)
        mpfd_.read_request(done);
    else
        done(Json());
}

tamed void Vrremote::call(Json msg, event<Json> done) {
    if (!mpfd_)
        twait { connect(tamer::rebind<bool>(make_event())); }
    if (mpfd_)
        mpfd_.call(msg, done);
    else
        done(Json());
}
#endif


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

tamed void go() {
    tamed {
        std::vector<Vrtestnode*> nodes;
        std::vector<Vrgroup*> groups;
    }
    for (int i = 0; i < 5; ++i)
        new Vrtestnode(Vrendpoint::make_uid(), nodes);
    for (int i = 0; i < 5; ++i)
        groups.push_back(new Vrgroup(nodes[i]->uid(), nodes[i]->listener()));
    for (int i = 0; i < 5; ++i)
        groups[i]->dump(std::cout);
    twait { groups[0]->join(Json::object("uid", nodes[1]->uid()),
                            tamer::rebind<Vrendpoint*>(make_event())); }
    for (int i = 0; i < 5; ++i)
        groups[i]->dump(std::cout);
    twait { tamer::at_delay_usec(10000, make_event()); }
    for (int i = 0; i < 5; ++i)
        groups[i]->dump(std::cout);
    twait { groups[0]->join(Json::object("uid", nodes[2]->uid()),
                            tamer::rebind<Vrendpoint*>(make_event())); }
    twait { tamer::at_delay_usec(10000, make_event()); }
    for (int i = 0; i < 5; ++i)
        groups[i]->dump(std::cout);
    twait { groups[4]->join(Json::object("uid", nodes[0]->uid()),
                            tamer::rebind<Vrendpoint*>(make_event())); }
    twait { tamer::at_delay_usec(10000, make_event()); }
    for (int i = 0; i < 5; ++i)
        groups[i]->dump(std::cout);
}

int main(int, char**) {
    tamer::initialize();
    go();
    tamer::loop();
    tamer::cleanup();
}
