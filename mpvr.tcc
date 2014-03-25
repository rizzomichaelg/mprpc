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
    m_vri_view_status,
    m_vri_view_confirm,
    m_vri_view_adopt,
    m_vri_error
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
//                 me: INDEX, primary: INDEX }

Vrgroup::Vrgroup(const String& group_name, Vrendpoint* me)
    : group_name_(group_name), me_(me), commitno_(0), new_view_count_(0) {
    view_.viewno = 0;
    view_.primary_index = 0;
    if (me_) {
        view_.members = Json::array(Json::object("uid", me->uid()));
        view_.my_index = 0;
        endpoints_[me->uid()] = me;
    } else {
        view_.members = Json::array();
        view_.my_index = -1;
    }
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
    }
}

bool Vrgroup::check_view_members(const Json& j) const {
    if (!j.is_array())
        return false;
    std::unordered_map<String, int> seen_uids;
    bool contains_me;
    for (auto it = j.abegin(); it != j.aend(); ++it) {
        if (!it->is_object()
            || !it->get("uid").is_string()
            || seen_uids.find(it->get("uid").to_s()) != seen_uids.end())
            return false;
        String uid = it->get("uid").to_s();
        seen_uids[uid] = 1;
        if (uid == this->uid())
            contains_me = true;
        else if (!it->get("addr").is_string()
                 || !it->get("port").is_int()
                 || it->get("port").to_i() <= 0
                 || it->get("port").to_i() > 65535)
            return false;
    }
    if (me_ && !contains_me)
        return false;
    return true;
}

bool Vrgroup::view_members_equal(const Json& a, const Json& b) {
    if (a.size() != b.size())
        return false;
    for (int i = 0; i != a.size(); ++i)
        if (a[i]["uid"] != b[i]["uid"])
            return false;
    return true;
}

inline Json& Vrgroup::view_type::find(const String& uid) {
    static Json thenull;
    for (int i = 0; i != members.size(); ++i)
        if (members[i].get("uid") == uid)
            return members[i];
    return thenull;
}

void Vrgroup::view_type::set_me(const String& uid) {
    my_index = -1;
    for (int i = 0; i != members.size(); ++i)
        if (members[i].get("uid") == uid)
            my_index = i;
}

void Vrgroup::send_view_status(Vrendpoint* who, bool status) {
    view_type& v = (new_view_count_ ? new_view_ : view_);
    Json j = Json::object("viewno", v.viewno,
                          "members", v.members,
                          "primary", v.primary_index,
                          "status", status);
    if (new_view_count_)
        j.set("flux", true);
    who->send(Json::array((int) m_vri_view_status, Json::null, j));
}

void Vrgroup::broadcast_view_status() {
    for (auto it = new_view_.members.abegin();
         it != new_view_.members.aend(); ++it) {
        Vrendpoint* e = endpoints_[it->get("uid").to_s()];
        if (e != me_)
            send_view_status(e, true);
    }
}

void Vrgroup::process_view_status(Vrendpoint* who, const Json& msg) {
    Json payload = msg[2];
    if (!payload.is_o()
        || !check_view_members(payload.get("members"))
        || !payload.get("viewno").is_i()
        || !payload.get("primary").is_i()
        || payload.get("primary").to_i() < 0
        || payload.get("primary").to_i() >= payload.get("members").size()) {
    error:
        who->send(Json::array((int) m_vri_error, -msg[1]));
        return;
    }

    view_type v;
    v.viewno = payload.get("viewno").to_u64();
    v.members = payload.get("members");
    v.primary_index = payload.get("primary").to_i();
    v.set_me(me_->uid());
    if (!v.find(who->uid()))    // view must include sender
        goto error;

    if (v.viewno < view_.viewno
        || (new_view_count_ && v.viewno < new_view_.viewno))
        send_view_status(who, false);
    else if (v.viewno == view_.viewno)
        send_view_status(who, view_members_equal(v.members, view_.members)
                         && v.primary_index == view_.primary_index);
    else if (!new_view_count_
             || v.viewno > new_view_.viewno) {
        new_view_count_ = 1 + (who != me_);
        new_view_confirmed_ = 0;
        new_view_ = v;
        broadcast_view_status();
    } else if (view_members_equal(v.members, new_view_.members)
               && v.primary_index == new_view_.primary_index) {
        Json& sender = new_view_.find(who->uid());
        if (!sender["gotstatus"]) {
            ++new_view_count_;
            sender["gotstatus"] = true;
        }
        send_view_status(who, true);
    } else
        send_view_status(who, false);

    if (new_view_count_ > new_view_.members.size() / 2)
        start_view_confirm();
}

Vrendpoint* Vrgroup::primary(const view_type& v) const {
    if ((unsigned) v.primary_index < (unsigned) v.members.size())
        return endpoints_[v.members[v.primary_index].get("uid").to_s()];
    else
        return nullptr;
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

void Vrgroup::start_view_adopt() {
    view_ = new_view_;
    new_view_count_ = new_view_confirmed_ = 0;

    for (int i = 0; i != view_.members.size(); ++i)
        if (i != view_.my_index) {
            Vrendpoint* e = endpoints_[view_.members[i]["uid"].to_s()];
            e->send(Json::array(m_vri_view_adopt, Json::null,
                                view_.viewno));
        }
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
        new Vrtestnode(Vrendpoint::make_uid());
    for (int i = 0; i < 5; ++i)
        groups.push_back(new Vrgroup(nodes[i]->uid(), nodes[i]->listener()));
    
}

int main(int argc, char** argv) {
    tamer::initialize();
    go();
    tamer::loop();
    tamer::cleanup();
}
