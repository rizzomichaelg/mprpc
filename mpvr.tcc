// -*- mode: c++ -*-
#include "clp.h"
#include "mpfd.hh"
#include "mpvr.hh"
#include <netdb.h>
#include <fcntl.h>
#include <unistd.h>
#include <algorithm>
#include <set>
#include <tamer/channel.hh>

enum {
    m_vri_request = 1,     // [1, xxx, seqno, request [, request]*]
    m_vri_response = 2,    // [2, xxx, [seqno, reply]*]
    m_vri_commit = 3,      // P->R: [3, xxx, viewno, commitno, logno, [client_uid, client_seqno, request]*]
                           // R->P: [3, xxx, viewno, storeno]
    m_vri_handshake = 4,   // [4, xxx, random_value]
    m_vri_join = 5,        // [5, xxx]
    m_vri_view = 6,        // [6, xxx, object]
    m_vri_error = 100
};

Vrconstants vrconstants;

std::ostream& operator<<(std::ostream& out, const timeval& tv) {
    char buf[40];
    int x = sprintf(buf, "%ld.%06ld", (long) tv.tv_sec, (long) tv.tv_usec);
    out.write(buf, x);
    return out;
}

String random_string(std::mt19937& rg) {
    uint64_t x = std::uniform_int_distribution<uint64_t>()(rg);
    return String((char*) &x, 6).encode_base64();
}

String Vrchannel::make_replica_uid() {
    static int counter;
    return String("n") + String(counter++);
}

String Vrchannel::make_client_uid() {
    static int counter;
    return String("c") + String(counter++);
}


// Handshake protocol (clients + interconnect)

tamed void handshake_protocol(Vrchannel* peer, bool active_end,
                              double message_timeout, double timeout,
                              tamer::event<bool> done) {
    tamed {
        Json msg;
        String peer_uid = peer->remote_uid();
        double start_time = tamer::drecent();
    }

    // handshake loop with retry
    while (1) {
        if (active_end) {
            Json handshake_msg = Json::array((int) m_vri_handshake, Json::null,
                                             peer->connection_uid());
            log_send(peer) << handshake_msg << "\n";
            peer->send(handshake_msg);
        }
        twait {
            peer->receive(tamer::add_timeout(message_timeout,
                                             make_event(msg)));
        }
        if (msg || tamer::drecent() >= start_time + timeout)
            break;
    }

    // test handshake message
    if (!msg) {
        log_receive(peer) << "handshake timeout\n";
        delete peer;
        done(false);
        return;
    } else if (!(msg.is_a() && msg.size() >= 3 && msg[0] == m_vri_handshake
                 && msg[2].is_s())) {
        log_receive(peer) << "bad handshake " << msg << "\n";
        delete peer;
        done(false);
        return;
    } else
        log_receive(peer) << msg << "\n";

    // parse handshake and respond
    String handshake_value = msg[2].to_s();
    assert(!peer->connection_uid()
           || peer->connection_uid() == handshake_value);
    peer->set_connection_uid(handshake_value);
    if (!active_end)
        peer->send(msg);

    done(true);
}


// Login protocol.
//   message m_vri_hello:
//     request:  { group: GROUPNAME, uid: UID }
//     response: { ok: true,
//                 members: [ {addr: ADDR, port: PORT, uid: UID}... ],
//                 me: INDEX, primary: INDEX }

Vrview::Vrview()
    : viewno(0), primary_index(0), my_index(-1), nacked(0), nconfirmed(0) {
}

Vrview Vrview::make_singular(String peer_uid, Json peer_name) {
    Vrview v;
    v.members.push_back(member_type(std::move(peer_uid),
                                    std::move(peer_name)));
    v.members[0].has_storeno = true;
    v.members[0].storeno = 0;
    v.members[0].store_count = 1;
    v.primary_index = v.my_index = 0;
    return v;
}

bool Vrview::assign(Json msg, const String& my_uid) {
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
        Json peer_name;
        if (it->is_object())
            peer_name = *it;
        else if (it->is_string())
            peer_name = Json::object("uid", *it);
        if (!peer_name.is_object()
            || !peer_name.get("uid").is_string()
            || !(uid = peer_name.get("uid").to_s())
            || seen_uids.find(uid) != seen_uids.end())
            return false;
        seen_uids[uid] = 1;
        if (uid == my_uid)
            my_index = it - membersj.abegin();
        members.push_back(member_type(uid, std::move(peer_name)));
    }

    return true;
}

inline int Vrview::count(const String& uid) const {
    for (auto it = members.begin(); it != members.end(); ++it)
        if (it->uid == uid)
            return 1;
    return 0;
}

inline Vrview::member_type* Vrview::find_pointer(const String& uid) {
    for (auto it = members.begin(); it != members.end(); ++it)
        if (it->uid == uid)
            return &*it;
    return nullptr;
}

inline Json Vrview::members_json() const {
    Json j = Json::array();
    for (auto it = members.begin(); it != members.end(); ++it)
        if (it->peer_name.size() == 1)
            j.push_back(it->uid);
        else
            j.push_back(it->peer_name);
    return j;
}

bool Vrview::operator==(const Vrview& x) const {
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

bool Vrview::shared_quorum(const Vrview& x) const {
    size_t nshared = 0;
    for (auto it = members.begin(); it != members.end(); ++it)
        if (x.count(it->uid))
            ++nshared;
    return nshared == size()
        || nshared == x.size()
        || (nshared > f() && nshared > x.f());
}

void Vrview::prepare(String uid, const Json& payload) {
    auto it = members.begin();
    while (it != members.end() && it->uid != uid)
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
        if (!payload["storeno"].is_null())
            account_commit(it.operator->(), payload["storeno"].to_u());
    }
}

void Vrview::clear_preparation() {
    nacked = nconfirmed = 0;
    for (auto it = members.begin(); it != members.end(); ++it)
        it->acked = it->confirmed = false;
}

void Vrview::add(String peer_uid, const String& my_uid) {
    auto it = members.begin();
    while (it != members.end() && it->uid < peer_uid)
        ++it;
    if (it == members.end() || it->uid != peer_uid)
        members.insert(it, member_type(std::move(peer_uid), Json()));

    my_index = -1;
    for (size_t i = 0; i != members.size(); ++i)
        if (members[i].uid == my_uid)
            my_index = i;

    advance();
}

void Vrview::advance() {
    clear_preparation();
    ++viewno;
    if (!viewno)
        ++viewno;
    primary_index = viewno % members.size();
}

Json Vrview::commits_json() const {
    Json j = Json::array();
    for (auto it = members.begin(); it != members.end(); ++it) {
        Json x = Json::array(it->uid);
        if (it->has_storeno)
            x.push_back_list(it->storeno.value(), it->store_count);
        bool is_primary = it - members.begin() == primary_index;
        bool is_me = it - members.begin() == my_index;
        if (is_primary || is_me)
            x.push_back(String(is_primary ? "p" : "") + String(is_me ? "*" : ""));
        j.push_back(x);
    }
    return j;
}

void Vrview::account_commit(member_type* peer, lognumber_t storeno) {
    assert(!peer->has_storeno || storeno >= peer->storeno);
    bool has_old_storeno = peer->has_storeno;
    lognumber_t old_storeno = peer->storeno;
    peer->has_storeno = true;
    peer->storeno = storeno;
    peer->store_count = 0;
    for (auto it = members.begin(); it != members.end(); ++it)
        if (it->has_storeno) {
            if (it->storeno <= storeno
                && (!has_old_storeno || it->storeno > old_storeno)
                && &*it != peer)
                ++it->store_count;
            if (storeno <= it->storeno)
                ++peer->store_count;
        }
}

bool Vrview::account_all_commits(Json old_cj) {
    bool changed = false;
    Json cj = commits_json();
    for (auto it = members.begin(); it != members.end(); ++it) {
        unsigned old_store_count = it->store_count;
        it->store_count = 0;
        for (auto jt = members.begin(); jt != members.end(); ++jt)
            if (it->has_storeno && jt->has_storeno
                && it->storeno <= jt->storeno)
                ++it->store_count;
        changed = changed || it->store_count != old_store_count;
    }
    if (old_cj)
        std::cerr << (changed ? "! " : ". ") << old_cj << " => " << cj << " => " << commits_json() << "\n";
    else
        std::cerr << "- " << commits_json() << "\n";
    return changed;
}



Vrnode::Vrnode(const String& group_name, Vrchannel* me, std::mt19937& rg)
    : group_name_(group_name), want_member_(!!me), me_(me),
      first_logno_(0),
      commitno_(0), complete_commitno_(0),
      stopped_(false), commit_sent_at_(0),
      rg_(rg) {
    if (me_) {
        cur_view_ = Vrview::make_singular(me_->local_uid(),
                                          me_->local_name());
        endpoints_[me->local_uid()] = me;
        listen_loop();
    }
    next_view_ = cur_view_;
}

void Vrnode::dump(std::ostream& out) const {
    timeval now = tamer::now();
    out << now << ":" << uid() << ": " << unparse_view_state()
        << " " << cur_view_.members_json()
        << " p@" << cur_view_.primary_index << "\n";
}

String Vrnode::unparse_view_state() const {
    StringAccum sa;
    sa << "v#" << cur_view_.viewno
       << (cur_view_.me_primary() ? "p" : "");
    if (next_view_.viewno != cur_view_.viewno)
        sa << "<v#" << next_view_.viewno
           << (next_view_.me_primary() ? "p" : "")
           << ":" << next_view_.nacked << "." << next_view_.nconfirmed << ">";
    sa << " ";
    if (!first_logno_ && log_.size() == 0) {
        assert(!commitno_);
        sa << "-";
    } else {
        if (first_logno_)
            sa << first_logno_;
        sa << ":";
        if (commitno_ != last_logno())
            sa << commitno_;
        sa << ":" << last_logno();
    }
    return sa.take_string();
}

tamed void Vrnode::listen_loop() {
    tamed { Vrchannel* peer; }
    while (1) {
        twait { me_->receive_connection(make_event(peer)); }
        if (!peer)
            break;
        connection_handshake(peer, false);
    }
}

tamed void Vrnode::connect(String peer_uid, event<> done) {
    tamed { Vrchannel* peer; Json peer_name; }
    assert(me_);

    // does peer already exist?
    if (endpoints_[peer_uid]) {
        done();
        return;
    }

    // are we already connecting?
    if (connection_wait_.count(peer_uid)) {
        connection_wait_[peer_uid] += std::move(done);
        return;
    }

    connection_wait_[peer_uid] = std::move(done);
    // random delay to reduce likelihood of simultaneous connection,
    // which we currently handle poorly
    twait { tamer::at_delay(rand01() / 100, make_event()); }

    // connected during delay?
    if (endpoints_[peer_uid]) {
        assert(!connection_wait_.count(peer_uid));
        return;
    }

    std::cerr << tamer::recent() << ":" << uid() << " <-> " << peer_uid
              << ": connecting\n";
    if (!(peer_name = node_names_[peer_uid]))
        peer_name = Json::object("uid", peer_uid);
    twait { me_->connect(peer_uid, peer_name, make_event(peer)); }
    if (peer) {
        assert(peer->remote_uid() == peer_uid);
        peer->set_connection_uid(random_string(rg_));
        connection_handshake(peer, true);
    } else {
        connection_wait_[peer_uid]();
        connection_wait_.erase(peer_uid);
    }
}

tamed void Vrnode::join(String peer_uid, event<> done) {
    tamed { Vrchannel* ep; }
    assert(want_member_ && next_view_.size() == 1);
    while (next_view_.size() == 1) {
        if ((ep = endpoints_[peer_uid])) {
            ep->send(Json::array((int) m_vri_join, Json::null));
            twait {
                at_view(next_view_.viewno + 1,
                        tamer::add_timeout(k_.message_timeout, make_event()));
            }
        } else
            twait { connect(peer_uid, make_event()); }
    }
    done();
}

void Vrnode::join(String peer_uid, Json peer_name, event<> done) {
    node_names_[peer_uid] = std::move(peer_name);
    return join(peer_uid, done);
}

tamed void Vrnode::connection_handshake(Vrchannel* peer, bool active_end) {
    tamed { bool ok; }

    twait {
        handshake_protocol(peer, active_end, k_.message_timeout,
                           k_.handshake_timeout, make_event(ok));
    }
    if (!ok)
        return;

    String peer_uid = peer->remote_uid();
    if (endpoints_[peer_uid]) {
        String old_cuid = endpoints_[peer_uid]->connection_uid();
        if (old_cuid < peer->connection_uid())
            log_connection(peer) << "preferring old connection (" << old_cuid << ")\n";
        else {
            log_connection(peer) << "dropping old connection (" << old_cuid << ")\n";
            endpoints_[peer_uid]->close();
            endpoints_[peer_uid] = peer;
        }
    } else
        endpoints_[peer_uid] = peer;

    connection_wait_[peer_uid]();
    connection_wait_.erase(peer_uid);
    connection_loop(peer);
}

tamed void Vrnode::connection_loop(Vrchannel* peer) {
    tamed { Json msg; }

    while (1) {
        twait { peer->receive(make_event(msg)); }
        if (!msg || !msg.is_a() || msg.size() < 2 || !msg[0].is_i())
            break;
        if (stopped_) // ignore message
            continue;
        log_receive(peer) << msg << " " << unparse_view_state() << "\n";
        if (msg[0] == m_vri_handshake)
            peer->send(msg);
        else if (msg[0] == m_vri_request)
            process_request(peer, msg);
        else if (msg[0] == m_vri_commit)
            process_commit(peer, msg);
        else if (msg[0] == m_vri_join)
            process_join(peer, msg);
        else if (msg[0] == m_vri_view)
            process_view(peer, msg);
    }

    log_connection(peer) << "connection closed\n";
    if (endpoints_[peer->remote_uid()] == peer)
        endpoints_.erase(peer->remote_uid());
    delete peer;
}

void Vrnode::at_view(viewnumber_t viewno, tamer::event<> done) {
    if (viewno > cur_view_.viewno)
        at_view_.push_back(std::make_pair(viewno, std::move(done)));
    else
        done();
}

void Vrnode::at_store(lognumber_t storeno, tamer::event<> done) {
    if (storeno > last_logno())
        at_store_.push_back(std::make_pair(storeno, std::move(done)));
    else
        done();
}

void Vrnode::at_commit(viewnumber_t commitno, tamer::event<> done) {
    if (commitno > commitno_)
        at_commit_.push_back(std::make_pair(commitno, std::move(done)));
    else
        done();
}

void Vrnode::process_view(Vrchannel* who, const Json& msg) {
    Json payload = msg[2];
    Vrview v;
    if (!v.assign(payload, uid())
        || !v.count(who->remote_uid())) {
        who->send(Json::array((int) m_vri_error, -msg[1]));
        return;
    }

    viewnumberdiff_t vdiff = (viewnumberdiff_t) (v.viewno - next_view_.viewno);
    int want_send;
    if (vdiff < 0
        || (vdiff == 0 && v != next_view_)
        || !next_view_.shared_quorum(v))
        want_send = 2;
    else if (vdiff == 0) {
        cur_view_.prepare(who->remote_uid(), payload);
        next_view_.prepare(who->remote_uid(), payload);
        if (payload["log"]
            && next_view_.me_primary()
            && cur_view_.viewno != next_view_.viewno)
            process_view_log_transfer(payload);
        want_send = !payload["ack"] && !payload["confirm"]
            && (cur_view_.viewno != next_view_.viewno || is_primary());
    } else {
        // start new view
        next_view_ = v;
        initialize_next_view();
        cur_view_.prepare(who->remote_uid(), payload);
        next_view_.prepare(who->remote_uid(), payload);
        broadcast_view();
        want_send = 0;
    }

    if (cur_view_.nacked > cur_view_.f()
        && next_view_.nacked > next_view_.f()
        && (next_view_.me_primary()
            || next_view_.primary().acked)
        && !next_view_sent_confirm_) {
        if (next_view_.me_primary())
            next_view_.prepare(uid(), Json::object("confirm", true));
        else
            send_view(next_view_.primary().uid);
        next_view_sent_confirm_ = true;
    }
    if (next_view_.nconfirmed > next_view_.f()
        && next_view_.me_primary()
        && want_send != 2) {
        if (cur_view_.viewno != next_view_.viewno) {
            next_view_.account_all_commits(Json());
            cur_view_ = next_view_;
            process_at_number(cur_view_.viewno, at_view_);
            primary_keepalive_loop();
            for (auto it = cur_view_.members.begin();
                 it != cur_view_.members.end(); ++it)
                if (it->confirmed)
                    send_commit_log(it->uid);
        } else
            send_commit_log(who->remote_uid());
    } else if (want_send)
        send_view(who);
}

void Vrnode::process_view_log_transfer(Json& payload) {
    assert(payload["storeno"].is_u()
           && payload["log"].is_a()
           && payload["log"].size() % 4 == 0);
    lognumber_t last_logno = payload["storeno"].to_u();
    lognumber_t logno = last_logno - payload["log"].size() / 4;
    const Json& log = payload["log"];
    assert(logno >= first_logno_);
    for (int i = 0; i != log.size(); i += 4, ++logno) {
        log_item li(log[i].to_u(), log[i+1].to_s(), log[i+2].to_u(), log[i+3]);
        auto it = log_.begin() + (logno - first_logno_);
        if (it == log_.end())
            log_.push_back(std::move(li));
        else if (it->viewno < li.viewno) {
            *it = std::move(li);
            next_view_logno_ = std::min(next_view_logno_, logno);
        } else if (it->viewno == li.viewno)
            assert(it->client_uid == li.client_uid
                   && it->client_seqno == li.client_seqno);
        else
            next_view_logno_ = std::min(next_view_logno_, logno);
    }
    process_at_number(first_logno_ + log.size(), at_store_);
}

Json Vrnode::view_payload(const String& peer_uid) {
    Json payload = Json::object("viewno", next_view_.viewno.value(),
                                "members", next_view_.members_json(),
                                "primary", next_view_.primary_index,
                                "commitno", commitno_.value(),
                                "storeno", last_logno().value());
    auto it = next_view_.members.begin();
    while (it != next_view_.members.end() && it->uid != peer_uid)
        ++it;
    if (it != next_view_.members.end() && it->acked)
        payload["ack"] = true;
    if (cur_view_.nacked > cur_view_.f()
        && next_view_.nacked > next_view_.f())
        payload["confirm"] = true;
    if (next_view_.viewno != cur_view_.viewno
        && !next_view_.me_primary()
        && next_view_.primary().has_storeno) {
        lognumber_t logno = next_view_.primary().storeno;
        Json log = Json::array();
        assert(logno >= first_logno_);
        if ((size_t) (logno - first_logno_) < log_.size())
            for (auto it = log_.begin() + (logno - first_logno_);
                 it != log_.end(); ++it)
                log.push_back_list(it->viewno.value(),
                                   it->client_uid,
                                   it->client_seqno,
                                   it->request);
        payload["log"] = log;
    }
    return payload;
}

void Vrnode::send_view(Vrchannel* who, Json payload, Json seqno) {
    if (!payload.get("members"))
        payload.merge(view_payload(who->remote_uid()));
    who->send(Json::array((int) m_vri_view, seqno, payload));
    log_send(who) << payload << " " << unparse_view_state() << "\n";
}

tamed void Vrnode::send_view(String peer_uid) {
    tamed { Json payload; Vrchannel* ep; }
    payload = view_payload(peer_uid);
    while (!(ep = endpoints_[peer_uid]))
        twait { connect(peer_uid, make_event()); }
    if (ep != me_)
        send_view(ep, payload);
}

void Vrnode::broadcast_view() {
    for (auto it = next_view_.members.begin();
         it != next_view_.members.end(); ++it)
        send_view(it->uid);
}

void Vrnode::process_join(Vrchannel* who, const Json&) {
    Vrview v;
    if (!next_view_.count(who->remote_uid())) {
        next_view_.add(who->remote_uid(), uid());
        start_view_change();
    }
}

void Vrnode::initialize_next_view() {
    cur_view_.clear_preparation();
    next_view_sent_confirm_ = false;
    next_view_logno_ = last_logno();
    Json my_msg = Json::object("storeno", next_view_logno_.value());
    cur_view_.prepare(uid(), my_msg);
    next_view_.prepare(uid(), my_msg);
}

tamed void Vrnode::start_view_change() {
    tamed { viewnumber_t view = next_view_.viewno; }
    initialize_next_view();
    broadcast_view();

    // kick off another view change if this one appears to fail
    twait { tamer::at_delay(k_.view_change_timeout * (1 + rand01() / 8),
                            make_event()); }
    if (cur_view_.viewno < view) {
        std::cerr << tamer::recent() << ":" << uid() << ": timing out view "
                  << unparse_view_state() << "\n";
        next_view_.advance();
        start_view_change();
    }
}

void Vrnode::process_request(Vrchannel* who, const Json& msg) {
    if (msg.size() < 4 || !msg[2].is_i())
        who->send(Json::array(0, msg[1], false));
    else if (!is_primary() || between_views())
        send_view(who, Json(), msg[1]);
    else {
        lognumber_t from_storeno = last_logno();
        unsigned seqno = msg[2].to_u64();
        for (int i = 3; i != msg.size(); ++i, ++seqno)
            log_.emplace_back(cur_view_.viewno, who->remote_uid(),
                              seqno, msg[i]);
        process_at_number(from_storeno, at_store_);
        broadcast_commit(from_storeno);

        // the new commits are replicated only here
        Vrview::member_type* my_member = &cur_view_.primary();
        my_member->storeno = last_logno();
        my_member->store_count = 1;
    }
}

inline Json Vrnode::commit_message(lognumber_t logno) const {
    return Json::array((int) m_vri_commit,
                       Json::null,
                       cur_view_.viewno.value(),
                       logno.value());
}

Json Vrnode::commit_log_message(lognumber_t from_storeno) const {
    Json msg = commit_message(commitno_);
    assert(from_storeno >= first_logno_);
    if (from_storeno < last_logno()) {
        msg.reserve(msg.size() + 1 + (last_logno() - from_storeno) * 3);
        msg.push_back(from_storeno.value());
        for (lognumber_t i = from_storeno; i != last_logno(); ++i) {
            const log_item& li = log_[i - first_logno_];
            msg.push_back_list(li.client_uid, li.client_seqno, li.request);
        }
    }
    return msg;
}

tamed void Vrnode::send_peer(String peer_uid, Json msg) {
    tamed { Vrchannel* ep = nullptr; Vrview::member_type* vp; }
    while (!(ep = endpoints_[peer_uid]))
        twait { connect(peer_uid, make_event()); }
    if (ep != me_)
        ep->send(msg);
}

void Vrnode::send_commit_log(String peer_uid) {
    if (Vrview::member_type* vp = cur_view_.find_pointer(peer_uid)) {
        Json msg = commit_log_message(vp->has_storeno ? vp->storeno
                                      : last_logno());
        send_peer(peer_uid, msg);
    }
}

void Vrnode::broadcast_commit(lognumber_t from_storeno) {
    assert(is_primary());
    Json msg = commit_log_message(from_storeno);
    for (auto it = cur_view_.members.begin();
         it != cur_view_.members.end(); ++it)
        send_peer(it->uid, msg);
    commit_sent_at_ = tamer::drecent();
}

void Vrnode::process_commit(Vrchannel* who, const Json& msg) {
    Vrview::member_type* peer = nullptr;
    if (msg.size() < 4
        || (msg.size() > 5 && (msg.size() - 5) % 3 != 0)
        || !msg[2].is_u()
        || !msg[3].is_u()) {
        who->send(Json::array(0, msg[1], false));
        return;
    }

    viewnumber_t view(msg[2].to_u());
    if (view == next_view_.viewno
        && between_views()) {
        assert(!next_view_.me_primary()
               && next_view_.primary().uid == who->remote_uid());
        cur_view_ = next_view_;
        next_view_sent_confirm_ = true;
        process_at_number(cur_view_.viewno, at_view_);
        backup_keepalive_loop();
    } else if (view != cur_view_.viewno
               || between_views()
               || (is_primary()
                   && !(peer = cur_view_.find_pointer(who->remote_uid())))) {
        send_view(who);
        return;
    }

    lognumber_t commitno = msg[3].to_u();
    if (is_primary()) {
        Json x = cur_view_.commits_json();
        cur_view_.account_commit(peer, commitno);
        assert(!cur_view_.account_all_commits(x));
        if (peer->store_count > cur_view_.f()
            && commitno > commitno_)
            update_commitno(commitno);
        if (peer->store_count == cur_view_.size()
            && commitno > complete_commitno_)
            complete_commitno_ = commitno;
    } else if (commitno > commitno_) {
        commitno_ = commitno;
        process_at_number(commitno_, at_commit_);
        primary_received_at_ = tamer::drecent();
    }

    if (!is_primary() && msg.size() > 5) {
        lognumber_t logno = msg[4].to_u();
        for (int i = 5; i != msg.size(); i += 3, ++logno) {
            size_t logpos = logno - first_logno_;
            if (commitno_ <= logno && logpos <= log_.size()) {
                if (logpos == log_.size())
                    log_.emplace_back(cur_view_.viewno,
                                      msg[i].to_s(),
                                      msg[i + 1].to_u(),
                                      msg[i + 2]);
                else if (log_[logpos].viewno < cur_view_.viewno) {
                    log_[logpos].viewno = cur_view_.viewno;
                    log_[logpos].client_uid = msg[i].to_s();
                    log_[logpos].client_seqno = msg[i + 1].to_u();
                    log_[logpos].request = msg[i + 2];
                }
            }
        }
        process_at_number(last_logno(), at_store_);
        who->send(commit_message(last_logno()));
    }
}

void Vrnode::update_commitno(lognumber_t commitno) {
    std::unordered_map<String, Json> messages;
    for (size_t i = commitno_.value(); i != commitno.value(); ++i) {
        log_item& li = log_[i - first_logno_.value()];
        Json& msg = messages[li.client_uid];
        if (!msg)
            msg = Json::array((int) m_vri_response, Json::null);
        msg.push_back(li.client_seqno).push_back(li.request);
    }
    commitno_ = commitno;
    process_at_number(commitno_, at_commit_);
    for (auto it = messages.begin(); it != messages.end(); ++it) {
        Vrchannel* ep = endpoints_[it->first];
        if (ep) {
            log_send(ep) << it->second << "\n";
            ep->send(std::move(it->second));
        }
    }
}

tamed void Vrnode::primary_keepalive_loop() {
    tamed { viewnumber_t view = cur_view_.viewno; }
    while (1) {
        twait { tamer::at_delay(k_.primary_keepalive_timeout / 4,
                                make_event()); }
        if (!in_view(view))
            break;
        if (tamer::drecent() - commit_sent_at_
              >= k_.primary_keepalive_timeout / 2
            && !stopped_)
            broadcast_commit(last_logno());
    }
}

tamed void Vrnode::backup_keepalive_loop() {
    tamed { viewnumber_t view = cur_view_.viewno; }
    primary_received_at_ = tamer::drecent();
    while (1) {
        twait { tamer::at_delay(k_.primary_keepalive_timeout * (0.375 + rand01() / 8),
                                make_event()); }
        if (next_view_.viewno != view)
            break;
        if (tamer::drecent() - primary_received_at_
              >= k_.primary_keepalive_timeout
            && !stopped_) {
            next_view_.advance();
            start_view_change();
            break;
        }
    }
}

void Vrnode::stop() {
    stopped_ = true;
}

void Vrnode::go() {
    stopped_ = false;
}


// Vrclient

Vrclient::Vrclient(Vrchannel* me, std::mt19937& rg)
    : uid_(random_string(rg)), client_seqno_(1), me_(me), channel_(nullptr),
      stopped_(false), rg_(rg) {
}

Vrclient::~Vrclient() {
    for (auto it = at_response_.begin(); it != at_response_.end(); ++it)
        it->second.unblock();
}

tamed void Vrclient::request(Json req, event<Json> done) {
    tamed { unsigned my_seqno = ++client_seqno_; }
    at_response_.push_back(std::make_pair(my_seqno, done));
    while (done) {
        if (channel_)
            channel_->send(Json::array((int) m_vri_request,
                                       Json::null,
                                       my_seqno,
                                       req));
        twait { tamer::at_delay(vrconstants.client_message_timeout,
                                make_event()); }
    }
}

tamed void Vrclient::connection_loop(Vrchannel* peer) {
    tamed { Json msg; }

    while (peer == channel_) {
        twait { peer->receive(make_event(msg)); }
        if (!msg || !msg.is_a() || msg.size() < 2 || !msg[0].is_i())
            break;
        if (stopped_) // ignore message
            continue;
        log_receive(peer) << msg << "\n";
        if (msg[0] == m_vri_handshake)
            peer->send(msg);
        else if (msg[0] == m_vri_response)
            process_response(msg);
        else if (msg[0] == m_vri_view)
            process_view(msg);
    }

    log_connection(peer) << "connection closed\n";
    delete peer;
}

void Vrclient::process_response(Json msg) {
    for (int i = 2; i != msg.size(); i += 2) {
        unsigned seqno = msg[i].to_u();
        auto it = at_response_.begin();
        while (it != at_response_.end() && circular_int<unsigned>::less(it->first, seqno))
            ++it;
        if (it != at_response_.end() && it->first == seqno)
            it->second(std::move(msg[i + 1]));
        while (!at_response_.empty() && !at_response_.front().second)
            at_response_.pop_front();
    }
}

void Vrclient::process_view(Json msg) {
    Vrview v;
    if (v.assign(msg[2], String())
        && (!channel_ || v.primary().uid != channel_->remote_uid())) {
        channel_ = nullptr;
        connect(v.primary().uid, v.primary().peer_name, event<bool>());
    }
}

tamed void Vrclient::connect(String peer_uid, Json peer_name,
                             event<bool> done) {
    tamed { Vrchannel* peer; bool ok; }
    twait { me_->connect(peer_uid, peer_name, make_event(peer)); }
    if (peer) {
        peer->set_connection_uid(random_string(rg_));
        twait { handshake_protocol(peer, true, vrconstants.message_timeout,
                                   10000, make_event(ok)); }
        if (ok) {
            channel_ = peer;
            connection_loop(peer);
        }
    }
    done(peer && channel_ == peer);
}


// Vrchannel

Json Vrchannel::local_name() const {
    return Json::object("uid", local_uid());
}

Json Vrchannel::remote_name() const {
    return Json::object("uid", remote_uid());
}

void Vrchannel::connect(String, Json, event<Vrchannel*>) {
    assert(0);
}

void Vrchannel::receive_connection(event<Vrchannel*>) {
    assert(0);
}

void Vrchannel::send(Json) {
    assert(0);
}

void Vrchannel::receive(event<Json>) {
    assert(0);
}

void Vrchannel::close() {
}


// Vrtestchannel

class Vrtestchannel;
class Vrtestlistener;
class Vrtestnode;
class Vrclient;

class Vrtestcollection {
  public:
    std::set<Vrtestchannel*> channels_;
    mutable std::mt19937 rg_;

    Vrnode* add_replica(const String& uid);
    Vrclient* add_client(const String& uid);
    Vrtestnode* operator[](const String& s) const {
        auto it = testnodes_.find(s);
        if (it != testnodes_.end())
            return it->second;
        else
            return nullptr;
    }
    double rand01() const {
        std::uniform_real_distribution<double> urd;
        return urd(rg_);
    }

    void check();

  private:
    std::unordered_map<String, Vrnode*> nodes_;
    std::unordered_map<String, Vrtestnode*> testnodes_;
};

class Vrtestnode {
  public:
    inline Vrtestnode(const String& uid, Vrtestcollection* collection);

    inline const String& uid() const {
        return uid_;
    }
    inline Json name() const {
        return Json::object("uid", uid_);
    }
    inline Vrtestlistener* listener() const {
        return listener_;
    }
    inline Vrtestcollection* collection() const {
        return collection_;
    }

    Vrtestchannel* connect(Vrtestnode* x);

  private:
    String uid_;
    Vrtestcollection* collection_;
    Vrtestlistener* listener_;
};

class Vrtestlistener : public Vrchannel {
  public:
    inline Vrtestlistener(String my_uid, Vrtestcollection* collection)
        : Vrchannel(my_uid, String()), collection_(collection) {
        set_connection_uid(my_uid);
    }
    void connect(String peer_uid, Json peer_name, event<Vrchannel*> done);
    void receive_connection(event<Vrchannel*> done);
  private:
    Vrtestcollection* collection_;
    tamer::channel<Vrchannel*> listenq_;
    friend class Vrtestnode;
};

class Vrtestchannel : public Vrchannel {
  public:
    Vrtestchannel(Vrtestnode* from, Vrtestnode* to);
    ~Vrtestchannel();
    inline void set_delay(double d);
    inline void set_loss(double p);
    inline Vrtestcollection* collection() const {
        return from_node_->collection();
    }
    void send(Json msg);
    void receive(event<Json> done);
    void close();
  private:
    Vrtestnode* from_node_;
    double delay_;
    double loss_p_;
    typedef std::pair<double, Json> message_t;
    std::deque<message_t> q_;
    std::deque<tamer::event<Json> > w_;
    Vrtestchannel* peer_;
    tamer::event<> coroutine_;
    tamer::event<> kill_coroutine_;
    tamed void coroutine();
    inline void do_send(Json msg);
    friend class Vrtestnode;
};

Vrnode* Vrtestcollection::add_replica(const String& uid) {
    assert(testnodes_.find(uid) == testnodes_.end());
    Vrtestnode* tn = new Vrtestnode(uid, this);
    testnodes_[uid] = tn;
    return nodes_[uid] = new Vrnode(tn->uid(), tn->listener(), rg_);
}

Vrclient* Vrtestcollection::add_client(const String& uid) {
    assert(testnodes_.find(uid) == testnodes_.end());
    Vrtestnode* tn = new Vrtestnode(uid, this);
    testnodes_[uid] = tn;
    return new Vrclient(tn->listener(), rg_);
}

Vrtestnode::Vrtestnode(const String& uid, Vrtestcollection* collection)
    : uid_(uid), collection_(collection) {
    listener_ = new Vrtestlistener(uid, collection);
}

Vrtestchannel* Vrtestnode::connect(Vrtestnode* n) {
    assert(n->uid() != uid());
    Vrtestchannel* my = new Vrtestchannel(this, n);
    Vrtestchannel* peer = new Vrtestchannel(n, this);
    my->peer_ = peer;
    peer->peer_ = my;
    n->listener()->listenq_.push_back(peer);
    collection_->channels_.insert(my);
    collection_->channels_.insert(peer);
    return my;
}

Vrtestchannel::Vrtestchannel(Vrtestnode* from, Vrtestnode* to)
    : Vrchannel(from->uid(), to->uid()), from_node_(from),
      delay_(0.05 + 0.0125 * from->collection()->rand01()), loss_p_(0.3) {
    coroutine();
}

Vrtestchannel::~Vrtestchannel() {
    close();
    while (!w_.empty()) {
        w_.front().unblock();
        w_.pop_front();
    }
    q_.clear();
    from_node_->collection()->channels_.erase(this);
}

void Vrtestchannel::close() {
    coroutine_();
    kill_coroutine_();
    if (peer_)
        peer_->peer_ = 0;
    peer_ = 0;
}

void Vrtestchannel::set_delay(double d) {
    delay_ = d;
}

void Vrtestchannel::set_loss(double p) {
    assert(p >= 0 && p <= 1);
    loss_p_ = p;
}

inline void Vrtestchannel::do_send(Json msg) {
    while (!w_.empty() && !w_.front())
        w_.pop_front();
    if (!w_.empty()
        && q_.empty()
        && delay_ <= 0) {
        w_.front()(std::move(msg));
        w_.pop_front();
    } else {
        q_.push_back(std::make_pair(tamer::drecent() + delay_,
                                    std::move(msg)));
        if (!w_.empty())
            coroutine_();
    }
}

void Vrtestchannel::send(Json msg) {
    if ((!loss_p_ || collection()->rand01() >= loss_p_) && peer_)
        peer_->do_send(std::move(msg));
}

void Vrtestchannel::receive(event<Json> done) {
    double now = tamer::drecent();
    if (w_.empty()
        && !q_.empty()
        && q_.front().first <= now) {
        done(std::move(q_.front().second));
        q_.pop_front();
    } else if (peer_) {
        w_.push_back(std::move(done));
        if (!q_.empty())
            coroutine_();
    } else
        done(Json());
}

tamed void Vrtestchannel::coroutine() {
    tvars {
        tamer::event<> kill;
        tamer::rendezvous<> rendez;
    }
    kill_coroutine_ = kill = rendez.make_event();
    while (kill) {
        while (!w_.empty() && !w_.front())
            w_.pop_front();
        if (!w_.empty() && !q_.empty()
            && tamer::drecent() >= q_.front().first) {
            w_.front()(std::move(q_.front().second));
            w_.pop_front();
            q_.pop_front();
        } else if (!w_.empty() && !q_.empty())
            twait { tamer::at_time(q_.front().first, make_event()); }
        else
            twait { coroutine_ = make_event(); }
    }
}

void Vrtestlistener::connect(String peer_uid, Json, event<Vrchannel*> done) {
    if (Vrtestnode* n = (*collection_)[peer_uid])
        done((*collection_)[local_uid()]->connect(n));
    else
        done(nullptr);
}

void Vrtestlistener::receive_connection(event<Vrchannel*> done) {
    listenq_.pop_front(done);
}




tamed void many_requests(Vrclient* client) {
    tamed { int n = 1; }
    while (1) {
        twait { client->request("req" + String(n), make_event()); }
        ++n;
        twait { tamer::at_delay(0.5, make_event()); }
    }
}

tamed void go() {
    tamed {
        Vrtestcollection vrg;
        std::vector<Vrnode*> nodes;
        Vrclient* client;
        Json j;
    }
    for (int i = 0; i < 5; ++i)
        nodes.push_back(vrg.add_replica(Vrchannel::make_replica_uid()));
    for (int i = 0; i < 5; ++i)
        nodes[i]->dump(std::cout);
    twait { nodes[0]->join(nodes[1]->uid(), make_event()); }
    for (int i = 0; i < 5; ++i)
        nodes[i]->dump(std::cout);
    twait {
        nodes[0]->at_view(1, make_event());
        nodes[1]->at_view(1, make_event());
    }

    for (int i = 0; i < 5; ++i)
        nodes[i]->dump(std::cout);
    twait { nodes[2]->join(nodes[0]->uid(), make_event()); }
    twait {
        nodes[0]->at_view(2, make_event());
        nodes[1]->at_view(2, make_event());
        nodes[2]->at_view(2, make_event());
    }

    for (int i = 0; i < 5; ++i)
        nodes[i]->dump(std::cout);
    twait { nodes[4]->join(nodes[0]->uid(), make_event()); }
    twait {
        nodes[0]->at_view(3, make_event());
        nodes[1]->at_view(3, make_event());
        nodes[2]->at_view(3, make_event());
        nodes[4]->at_view(3, make_event());
    }
    for (int i = 0; i < 5; ++i)
        nodes[i]->dump(std::cout);

    client = vrg.add_client(Vrchannel::make_client_uid());
    twait { client->connect(nodes[0]->uid(), make_event()); }
    many_requests(client);
    twait { tamer::at_delay_usec(10000, make_event()); }
    twait { tamer::at_delay_sec(3, make_event()); }
    nodes[4]->stop();
    twait { tamer::at_delay_sec(5, make_event()); }
    nodes[4]->go();

    twait { tamer::at_delay_sec(100000, make_event()); }
}

int main(int, char**) {
    tamer::set_time_type(tamer::time_virtual);
    tamer::initialize();
    go();
    tamer::loop();
    tamer::cleanup();
}
