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
    m_vri_connect = 0,
    m_vri_view_status = 1,
    m_vri_view_confirm = 2,
    m_vri_view_adopt = 3,
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
    : viewno(0), primary_index(0), my_index(-1) {
}

bool Vrgroup::view_type::assign(Json msg, const String& my_uid) {
    if (!msg.is_o())
        return false;
    Json viewnoj = msg["viewno"];
    Json membersj = msg["members"];
    Json primaryj = msg["primary"];
    if (!(viewnoj.is_i() && viewnoj >= 0
          && membersj.is_a()
          && primaryj.is_i() && primaryj >= 0 && primaryj < membersj.size()))
        return false;

    viewno = viewnoj.to_u64();
    members = membersj;
    primary_index = primaryj.to_i();
    my_index = -1;

    std::unordered_map<String, int> seen_uids;
    String uid;
    for (auto it = members.abegin(); it != members.aend(); ++it) {
        if (!it->is_object()
            || !it->get("uid").is_string()
            || !(uid = it->get("uid").to_s())
            || seen_uids.find(uid) != seen_uids.end())
            return false;
        seen_uids[uid] = 1;
        if (uid == my_uid)
            my_index = it - members.abegin();
        it->erase("prepared");
        it->erase("confirmed");
        it->erase("statusat");
        /*else if (!it->get("addr").is_string()
          || !it->get("port").is_int()
          || it->get("port").to_i() <= 0
                 || it->get("port").to_i() > 65535)
                 return false;*/
    }

    return true;
}

Json& Vrgroup::view_type::find(const String& uid) {
    static Json thenull;
    for (int i = 0; i != members.size(); ++i)
        if (members[i].get("uid") == uid)
            return members[i];
    return thenull;
}

inline String Vrgroup::view_type::primary_uid() const {
    return members[primary_index]["uid"].to_s();
}

bool Vrgroup::view_type::operator==(const view_type& x) const {
    if (viewno != x.viewno
        || primary_index != x.primary_index
        || my_index != x.my_index
        || members.size() != x.members.size())
        return false;
    for (int i = 0; i != members.size(); ++i)
        if (members[i]["uid"] != x.members[i]["uid"])
            return false;
    return true;
}

bool Vrgroup::view_type::shared_quorum(const view_type& x) const {
    int nshared = 0;
    for (auto it = members.abegin(); it != members.aend(); ++it)
        if (x.find(it->get("uid").to_s()))
            ++nshared;
    return nshared > members.size() / 2 && nshared > x.members.size() / 2;
}


Vrgroup::Vrgroup(const String& group_name, Vrendpoint* me)
    : group_name_(group_name), me_(me), commitno_(0), new_view_count_(0) {
    if (me_) {
        cur_view_.members = Json::array(Json::object("uid", me->uid()));
        cur_view_.my_index = 0;
        endpoints_[me->uid()] = me;
        listen_loop();
    }
    next_view_ = cur_view_;
}

void Vrgroup::dump(std::ostream& out) const {
    timeval now = tamer::now();
    out << now << ":" << uid() << ": " << unparse_view_state()
        << " " << view_.members << " p@" << view_.primary_index << "\n";
}

String Vrgroup::unparse_view_state() const {
    StringAccum sa;
    sa << "v#" << cur_view_.viewno
       << (cur_view_.primary_index == cur_view_.my_index ? "p" : "");
    if (new_view_count_)
        sa << "<v#" << next_view_.viewno
           << (next_view_.primary_index == next_view_.my_index ? "p" : "")
           << ":" << new_view_count_ << "." << new_view_confirmed_ << ">";
    return sa.take_string();
}

tamed void Vrgroup::listen_loop() {
    tamed { Vrendpoint* e; }
    while (1) {
        twait { me_->receive_connection(make_event(e)); }
        if (!e)
            break;
        if (endpoints_[e->uid()])
            delete endpoints_[e->uid()];
        endpoints_[e->uid()] = e;
        interconnect_loop(e);
    }
}

tamed void Vrgroup::connect(Json peer_name, event<Vrendpoint*> done) {
    tamed { Vrendpoint* peer; Json j; }
    assert(me_);
    if (peer_name.is_s())
        peer_name = Json::object("uid", peer_name);
    twait { me_->connect(peer_name, make_event(peer)); }
    if (peer) {
        peer_name["uid"] = peer->uid();
        if (endpoints_[peer->uid()])
            delete endpoints_[peer->uid()];
        endpoints_[peer->uid()] = peer;

        view_type v = new_view_count_ ? next_view_ : cur_view_;
        auto it = v.members.abegin();
        while (it != v.members.aend() && (*it)["uid"].to_s() < peer->uid())
            ++it;
        if (it == v.members.aend() || (*it)["uid"].to_s() != peer->uid()) {
            v.members.insert(it, peer_name);
            new_view_count_ = 1;
            ++v.viewno;
            v.primary_index = v.viewno % v.members.size();
            new_view_ = v;
            send_view_status(peer, true);
        }

        interconnect_loop(peer);
        done(peer);
    } else
        done(nullptr);
}

tamed void Vrgroup::interconnect_loop(Vrendpoint* peer) {
    tamed { Json msg; }
    while (1) {
        twait { peer->receive(make_event(msg)); }
        std::cout << tamer::now() << ":" << uid() << " <- " << peer->uid()
                  << ": recv " << msg << " " << unparse_view_state() << "\n";
        if (!msg || !msg.is_a() || msg.size() < 2 || !msg[0].is_i())
            break;
        if (msg[0] == m_vri_view_status)
            process_view_status(peer, msg);
        else if (msg[0] == m_vri_view_confirm)
            process_view_confirm(peer, msg);
        else if (msg[0] == m_vri_view_adopt)
            process_view_adopt(peer, msg);
    }
}

Vrendpoint* Vrgroup::primary(const view_type& v) const {
    if ((unsigned) v.primary_index < (unsigned) v.members.size())
        return endpoints_[v.members[v.primary_index].get("uid").to_s()];
    else
        return nullptr;
}

void Vrgroup::process_view_status(Vrendpoint* who, const Json& msg) {
    Json payload = msg[2];
    view_type v;
    if (!v.assign(payload, me_->uid())
        || !v.find(who->uid())) {
    error:
        who->send(Json::array((int) m_vri_error, -msg[1]));
        return;
    }

    viewnumberdiff_t vdiff = (viewnumberdiff_t) (v.viewno - next_view_.viewno);
    if (vdiff < 0
        || (vdiff == 0 && v != next_view_)
        || !next_view_.shared_quorum(v))
        send_view_status(who);
    else if (vdiff == 0 && cur_view_.viewno == next_view_.viewno)
        /* send nothing */;
    else if (vdiff == 0) {
        Json& remote = next_view_.find(who->uid());
        if (!remote["prepared"]) {
            remote["prepared"] = true;
            ++next_view_prepared_;
        }
        if (payload["confirm"]
            && !remote["confirmed"]) {
            remote["confirmed"] = true;
            ++next_view_confirmed_;
        }
        if (!payload["ackprepare"])
            send_view_status(who);
    } else {
        // start new view
        next_view_prepared_ = 2;
        next_view_confirmed_ = v.my_index == v.primary_index;
        next_view_sent_confirm_ = v.my_index == v.primary_index;
        next_view_ = v;
        next_view_.find(who->uid()).set("prepared", 1);
        broadcast_view_status();
    }

    if (next_view_prepared_ > next_view_.members.size() / 2
        && !next_view_sent_confirm_) {
        send_view_status(primary());
        next_view_sent_confirm_ = true;
    }
    if (next_view_confirmed_ == next_view_.members.size()
        && next_view_.my_index == next_view_.primary_index) {
        broadcast_view_adopted();
        next_view_prepared_ = next_view_confirmed_ = 0;
    }
}

void Vrgroup::send_view_status(Vrendpoint* who, bool status) {
    Json payload = Json::object("viewno", next_view_.viewno,
                                "members", next_view_.members,
                                "primary", next_view_.primary_index);
    Json& found = next_view_.find(who->uid());
    if (found && found["prepared"])
        payload["ackprepare"] = true;
    if (next_view_prepared_ > next_view_.members.size() / 2)
        payload["confirm"] = true;
        && found["statusat"].to_d() >= tamer::drecent() - 3)
        return;
    else if (found)
        found["statusat"] = tamer::drecent();
    if (new_view_count_)
        j.set("flux", true);
    who->send(Json::array((int) m_vri_view_status, Json::null, j));
}

tamed void Vrgroup::send_view_status(Json peer_name, bool status) {
    tamed { Vrendpoint* ep; }
    if (!(ep = endpoints_[peer_name["uid"].to_s()]))
        twait { connect(peer_name, make_event(ep)); }
    if (ep && ep != me_)
        send_view_status(ep, true);
}

void Vrgroup::broadcast_view_status() {
    for (auto it = new_view_.members.abegin();
         it != new_view_.members.aend(); ++it)
        send_view_status(*it, true);
}

void Vrgroup::start_view_confirm() {
    new_view_confirmed_ = (new_view_.primary_index == new_view_.my_index);
    if (new_view_.primary_index != new_view_.my_index) {
        Vrendpoint* e = primary(new_view_);
        e->send(Json::array((int) m_vri_view_confirm, Json::null,
                            new_view_.viewno));
    }

    if (new_view_confirmed_ > new_view_.members.size() / 2)
        start_view_adopt();
}

void Vrgroup::process_view_confirm(Vrendpoint* who, const Json& msg) {
    Json payload = msg[2];
    if (!payload.is_i()) {
    error:
        who->send(Json::array((int) m_vri_error, -msg[1]));
        return;
    }

    size_t new_viewno = payload.to_u64();
    if ((!new_view_count_ && new_viewno != view_.viewno)
        || (new_view_count_ && new_viewno != new_view_.viewno))
        goto error;

    if (new_view_count_) {
        Json& sender = new_view_.find(who->uid());
        if (!sender)
            goto error;
        if (!sender["gotconfirm"]) {
            sender["gotconfirm"] = true;
            ++new_view_confirmed_;
        }
    }

    if (new_view_confirmed_ > new_view_.members.size() / 2)
        start_view_adopt();
}

tamed void Vrgroup::send_view_adopt(Json peer_name, size_t viewno) {
    tamed { Vrendpoint* ep; }
    if (!(ep = endpoints_[peer_name["uid"].to_s()]))
        twait { connect(peer_name, make_event(ep)); }
    if (ep && ep != me_ && view_.viewno == viewno)
        ep->send(Json::array(m_vri_view_adopt, Json::null, viewno));
}

void Vrgroup::start_view_adopt() {
    view_ = new_view_;
    new_view_count_ = new_view_confirmed_ = 0;

    for (int i = 0; i != view_.members.size(); ++i)
        if (i != view_.my_index)
            send_view_adopt(view_.members[i], view_.viewno);
}

void Vrgroup::process_view_adopt(Vrendpoint* who, const Json& msg) {
    Json payload = msg[2];
    if (!payload.is_i()) {
    error:
        who->send(Json::array((int) m_vri_error, -msg[1]));
        return;
    }

    size_t viewno = payload.to_u64();
    if (viewno == view_.viewno
        && who->uid() == view_.primary_uid())
        /* OK, but do nothing */;
    else if (new_view_count_
             && viewno == new_view_.viewno
             && who->uid() == new_view_.primary_uid()) {
        view_ = new_view_;
        new_view_count_ = 0;
    } else
        goto error;
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

    inline void add_channel(Vrtestconnection* c) {
        channels_.push_back(c);
    }
    inline void remove_channel(Vrtestconnection* c) {
        auto it = std::find(channels_.begin(), channels_.end(), c);
        if (it != channels_.end()) {
            *it = channels_.back();
            channels_.pop_back();
        }
    }

  private:
    String uid_;
    std::vector<Vrtestconnection*> channels_;
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
    from_node_->remove_channel(this);
    if (peer_) {
        peer_->peer_ = 0;
        while (!peer_->q_.wait_empty())
            peer_->q_.push_back(Json());
    }
}

void Vrtestconnection::send(Json msg) {
    if (peer_) {
        std::cout << tamer::now() << ":"
                  << from_node_->uid() << " -> " << uid() << ": send " << msg << "\n";
        peer_->q_.push_back(msg);
    }
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
    return Json::null;
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
    twait { groups[0]->connect(Json::object("uid", nodes[1]->uid()),
                               tamer::rebind<Vrendpoint*>(make_event())); }
    for (int i = 0; i < 5; ++i)
        groups[i]->dump(std::cout);
    twait { tamer::at_delay_usec(10000, make_event()); }
    for (int i = 0; i < 5; ++i)
        groups[i]->dump(std::cout);
    twait { groups[0]->connect(Json::object("uid", nodes[2]->uid()),
                               tamer::rebind<Vrendpoint*>(make_event())); }
    twait { tamer::at_delay_usec(10000, make_event()); }
    for (int i = 0; i < 5; ++i)
        groups[i]->dump(std::cout);
    twait { groups[4]->connect(Json::object("uid", nodes[0]->uid()),
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
