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

std::ostream& operator<<(std::ostream& out, const timeval& tv) {
    char buf[40];
    int x = sprintf(buf, "%ld.%06ld", (long) tv.tv_sec, (long) tv.tv_usec);
    out.write(buf, x);
    return out;
}

String random_string() {
    FILE* f = fopen("/dev/urandom", "rb");
    uint64_t x = (uint64_t) (tamer::dnow() * 1000000);
    size_t n = fread(&x, sizeof(x), 1, f);
    assert(n == 1);
    fclose(f);
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


// Login protocol.
//   message m_vri_hello:
//     request:  { group: GROUPNAME, uid: UID }
//     response: { ok: true,
//                 members: [ {addr: ADDR, port: PORT, uid: UID}... ],
//                 me: INDEX, primary: INDEX }

Vrnode::view_type::view_type()
    : viewno(0), primary_index(0), my_index(-1), nacked(0), nconfirmed(0) {
}

Vrnode::view_type Vrnode::view_type::make_singular(String peer_uid) {
    view_type v;
    v.members.push_back(view_member(std::move(peer_uid), Json()));
    v.members[0].has_storeno = true;
    v.members[0].storeno = 0;
    v.members[0].store_count = 1;
    v.primary_index = v.my_index = 0;
    return v;
}

bool Vrnode::view_type::assign(Json msg, const String& my_uid) {
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
        members.push_back(view_member(uid, *it));
    }

    return true;
}

inline int Vrnode::view_type::count(const String& uid) const {
    for (auto it = members.begin(); it != members.end(); ++it)
        if (it->uid == uid)
            return 1;
    return 0;
}

inline Vrnode::view_member* Vrnode::view_type::find_pointer(const String& uid) {
    for (auto it = members.begin(); it != members.end(); ++it)
        if (it->uid == uid)
            return &*it;
    return nullptr;
}

inline Json Vrnode::view_type::members_json() const {
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

bool Vrnode::view_type::operator==(const view_type& x) const {
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

bool Vrnode::view_type::shared_quorum(const view_type& x) const {
    size_t nshared = 0;
    for (auto it = members.begin(); it != members.end(); ++it)
        if (x.count(it->uid))
            ++nshared;
    return nshared == size()
        || nshared == x.size()
        || (nshared > f() && nshared > x.f());
}

void Vrnode::view_type::prepare(String uid, const Json& payload) {
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
        if (!payload["storeno"].is_null()) {
            it->has_storeno = true;
            it->storeno = payload["storeno"].to_u();
        }
    }
}

void Vrnode::view_type::clear_preparation() {
    nacked = nconfirmed = 0;
    for (auto it = members.begin(); it != members.end(); ++it)
        it->acked = it->confirmed = false;
}

void Vrnode::view_type::add(String peer_uid, const String& my_uid) {
    auto it = members.begin();
    while (it != members.end() && it->uid < peer_uid)
        ++it;
    if (it == members.end() || it->uid != peer_uid)
        members.insert(it, view_member(std::move(peer_uid), Json()));

    my_index = -1;
    for (size_t i = 0; i != members.size(); ++i)
        if (members[i].uid == my_uid)
            my_index = i;

    advance();
}

void Vrnode::view_type::advance() {
    clear_preparation();
    ++viewno;
    if (!viewno)
        ++viewno;
    primary_index = viewno % members.size();
}

Json Vrnode::view_type::commits_json() const {
    Json j = Json::array();
    for (auto it = members.begin(); it != members.end(); ++it) {
        Json x = Json::array(it->uid);
        if (it->has_storeno)
            x.push_back(it->storeno.value()).push_back(it->store_count);
        bool is_primary = it - members.begin() == primary_index;
        bool is_me = it - members.begin() == my_index;
        if (is_primary || is_me)
            x.push_back(String(is_primary ? "p" : "") + String(is_me ? "*" : ""));
        j.push_back(x);
    }
    return j;
}

void Vrnode::view_type::account_commit(view_member* peer, lognumber_t storeno) {
    lognumber_t old_storeno = peer->storeno;
    peer->has_storeno = true;
    peer->storeno = storeno;
    peer->store_count = 0;
    for (auto it = members.begin(); it != members.end(); ++it) {
        if (it->storeno <= storeno
            && it->storeno > old_storeno
            && &*it != peer)
            ++it->store_count;
        if (storeno <= it->storeno)
            ++peer->store_count;
    }
}

bool Vrnode::view_type::account_all_commits() {
    bool changed = false;
    for (auto it = members.begin(); it != members.end(); ++it) {
        unsigned old_store_count = it->store_count;
        it->store_count = 0;
        for (auto jt = members.begin(); jt != members.end(); ++jt)
            if (it->has_storeno && jt->has_storeno
                && it->storeno <= jt->storeno)
                ++it->store_count;
        changed = changed || it->store_count != old_store_count;
    }
    return changed;
}



Vrnode::Vrnode(const String& group_name, Vrchannel* me, std::mt19937& rg)
    : group_name_(group_name), want_member_(!!me), me_(me),
      first_logno_(0),
      commitno_(0), complete_commitno_(0), broadcast_commitno_(0),
      stopped_(false),
      message_timeout_(1),
      handshake_timeout_(5),
      primary_keepalive_timeout_(1),
      backup_keepalive_timeout_(2),
      view_change_timeout_(0.5),
      commit_sent_at_(0),
      rg_(rg) {
    if (me_) {
        cur_view_ = view_type::make_singular(me_->local_uid());
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
    tamed { Vrchannel* peer; }
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
    twait { tamer::at_delay(rand01() / 50, make_event()); }

    // connected during delay?
    if (endpoints_[peer_uid]) {
        assert(!connection_wait_.count(peer_uid));
        return;
    }

    std::cerr << tamer::recent() << ":" << uid() << " <-> " << peer_uid
              << ": connecting\n";
    twait { me_->connect(peer_uid, make_event(peer)); }
    if (peer) {
        assert(peer->remote_uid() == peer_uid);
        peer->set_connection_uid(random_string());
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
                        tamer::add_timeout(message_timeout_, make_event()));
            }
        } else
            twait { connect(peer_uid, make_event()); }
    }
    done();
}

tamed void Vrnode::connection_handshake(Vrchannel* peer, bool active_end) {
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
            double timeout = active_end ? message_timeout_ : handshake_timeout_;
            peer->receive(tamer::add_timeout(timeout, make_event(msg)));
        }
        if (msg || tamer::drecent() >= start_time + handshake_timeout_)
            break;
    }

    // test handshake message
    if (!msg) {
        log_receive(peer) << "handshake timeout\n";
        delete peer;
        return;
    } else if (!(msg.is_a() && msg.size() >= 3 && msg[0] == m_vri_handshake
                 && msg[2].is_s())) {
        log_receive(peer) << "bad handshake " << msg << "\n";
        delete peer;
        return;
    } else
        log_receive(peer) << msg << "\n";

    // parse handshake
    String handshake_value = msg[2].to_s();
    assert(!peer->connection_uid()
           || peer->connection_uid() == handshake_value);
    peer->set_connection_uid(handshake_value);

    if (endpoints_[peer_uid]) {
        String old_cuid = endpoints_[peer_uid]->connection_uid();
        if (old_cuid < handshake_value)
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
    if (!active_end)
        peer->send(msg);

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
    if (storeno > first_logno_ + log_.size())
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
    view_type v;
    if (!v.assign(payload, uid())
        || !v.count(who->remote_uid())) {
        who->send(Json::array((int) m_vri_error, -msg[1]));
        return;
    }

    viewnumberdiff_t vdiff = (viewnumberdiff_t) (v.viewno - next_view_.viewno);
    bool want_send = false;
    if (vdiff < 0
        || (vdiff == 0 && v != next_view_)
        || !next_view_.shared_quorum(v))
        want_send = true;
    else if (vdiff == 0) {
        cur_view_.prepare(who->remote_uid(), payload);
        next_view_.prepare(who->remote_uid(), payload);
        if (payload["log"] && next_view_.me_primary())
            process_view_log_transfer(payload);
        want_send = !payload["ack"] && !payload["confirm"]
            && (cur_view_.viewno != next_view_.viewno || is_primary());
    } else {
        // start new view
        cur_view_.clear_preparation();
        next_view_sent_confirm_ = false;
        next_view_ = v;
        cur_view_.prepare(uid(), payload);
        next_view_.prepare(uid(), payload);
        cur_view_.prepare(who->remote_uid(), payload);
        next_view_.prepare(who->remote_uid(), payload);
        broadcast_view();
    }

    if (cur_view_.nacked > cur_view_.f()
        && next_view_.nacked > next_view_.f()
        && (next_view_.me_primary()
            || next_view_.primary().acked)
        && !next_view_sent_confirm_) {
        if (next_view_.me_primary())
            next_view_.prepare(uid(), Json::object("confirm", true));
        else
            send_view(next_view_.primary_uid());
        next_view_sent_confirm_ = true;
    }
    if (next_view_.nconfirmed > next_view_.f()
        && next_view_.me_primary()) {
        if (cur_view_.viewno != next_view_.viewno) {
            next_view_.account_all_commits();
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
        else if (it->viewno < li.viewno)
            *it = std::move(li);
        else if (it->viewno == li.viewno)
            assert(it->client_uid == li.client_uid
                   && it->client_seqno == li.client_seqno);
    }
    process_at_number(first_logno_ + log.size(), at_store_);
}

Json Vrnode::view_payload(const String& peer_uid) {
    Json payload = Json::object("viewno", next_view_.viewno.value(),
                                "members", next_view_.members_json(),
                                "primary", next_view_.primary_index,
                                "commitno", commitno_.value(),
                                "storeno", (first_logno_ + log_.size()).value());
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
        assert(logno >= first_logno_
               && first_logno_ + log_.size() >= logno);
        Json log = Json::array();
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
    view_type v;
    if (!next_view_.count(who->remote_uid())) {
        next_view_.add(who->remote_uid(), uid());
        start_view_change();
    }
}

tamed void Vrnode::start_view_change() {
    tamed { viewnumber_t view = next_view_.viewno; }
    cur_view_.clear_preparation();
    next_view_sent_confirm_ = false;
    cur_view_.prepare(uid(), Json());
    next_view_.prepare(uid(), Json());
    broadcast_view();

    // kick off another view change if this one appears to fail
    twait { tamer::at_delay(view_change_timeout_ * (1 + rand01() / 8),
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
        lognumber_t from_storeno = first_logno_ + log_.size();
        unsigned seqno = msg[2].to_u64();
        for (int i = 3; i != msg.size(); ++i, ++seqno)
            log_.emplace_back(cur_view_.viewno, who->remote_uid(),
                              seqno, msg[i]);
        process_at_number(from_storeno, at_store_);
        broadcast_commit(from_storeno);

        // the new commits are replicated only here
        view_member* my_member = &cur_view_.primary();
        my_member->storeno = first_logno_ + log_.size();
        my_member->store_count = 1;
    }
}

inline Json Vrnode::commit_message() const {
    return Json::array((int) m_vri_commit,
                       Json::null,
                       cur_view_.viewno.value(),
                       commitno_.value());
}

Json Vrnode::commit_log_message(lognumber_t from_storeno) const {
    Json msg = commit_message();
    lognumber_t last_logno = first_logno_ + log_.size();
    if (from_storeno != last_logno) {
        msg.reserve(msg.size() + 1 + (last_logno - from_storeno) * 3);
        msg.push_back(from_storeno.value());
        for (lognumber_t i = from_storeno; i != last_logno; ++i) {
            const log_item& li = log_[i - first_logno_];
            msg.push_back_list(li.client_uid, li.client_seqno, li.request);
        }
    }
    return msg;
}

tamed void Vrnode::send_peer(String peer_uid, Json msg) {
    tamed { Vrchannel* ep = nullptr; view_member* vp; }
    while (!(ep = endpoints_[peer_uid]))
        twait { connect(peer_uid, make_event()); }
    if (ep != me_)
        ep->send(msg);
}

void Vrnode::send_commit_log(String peer_uid) {
    if (view_member* vp = cur_view_.find_pointer(peer_uid)) {
        Json msg =  commit_log_message(vp->has_storeno ? vp->storeno
                                       : first_logno_ + log_.size());
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
    view_member* peer = nullptr;
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
               && next_view_.primary_uid() == who->remote_uid());
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
        cur_view_.account_commit(peer, commitno);
        assert(!cur_view_.account_all_commits());
        if (peer->store_count > cur_view_.f()
            && commitno > commitno_)
            update_commitno(commitno);
        if (peer->store_count == cur_view_.size()
            && commitno > complete_commitno_)
            complete_commitno_ = commitno;
    } else {
        assert(commitno >= commitno_);
        commitno_ = commitno;
        process_at_number(commitno_, at_commit_);
        primary_received_at_ = tamer::drecent();
    }

    if (!is_primary() && msg.size() > 5) {
        lognumber_t logno = msg[4].to_u();
        for (int i = 5; i != msg.size(); i += 3, ++logno)
            if (commitno_ <= logno) {
                size_t logpos = logno - first_logno_;
                while (logpos >= log_.size())
                    log_.push_back(log_item());
                log_[logpos].viewno = cur_view_.viewno;
                log_[logpos].client_uid = msg[i].to_s();
                log_[logpos].client_seqno = msg[i + 1].to_u();
                log_[logpos].request = msg[i + 2];
            }
        process_at_number(first_logno_ + log_.size(), at_store_);
        who->send(Json::array((int) m_vri_commit,
                              Json::null,
                              cur_view_.viewno.value(),
                              (first_logno_ + log_.size()).value()));
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
        if (ep)
            ep->send(std::move(it->second));
    }
}

tamed void Vrnode::primary_keepalive_loop() {
    tamed { viewnumber_t view = cur_view_.viewno; }
    while (1) {
        twait { tamer::at_delay(primary_keepalive_timeout_ / 4,
                                make_event()); }
        if (!in_view(view))
            break;
        if (tamer::drecent() - commit_sent_at_
              >= primary_keepalive_timeout_ / 2
            && !stopped_)
            broadcast_commit(first_logno_ + log_.size());
    }
}

tamed void Vrnode::backup_keepalive_loop() {
    tamed { viewnumber_t view = cur_view_.viewno; }
    primary_received_at_ = tamer::drecent();
    while (1) {
        twait { tamer::at_delay(primary_keepalive_timeout_ * (0.375 + rand01() / 8),
                                make_event()); }
        if (next_view_.viewno != view)
            break;
        if (tamer::drecent() - primary_received_at_
              >= primary_keepalive_timeout_
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


// Vrchannel

Json Vrchannel::local_name() const {
    return Json::object("uid", local_uid());
}

Json Vrchannel::remote_name() const {
    return Json::object("uid", remote_uid());
}

void Vrchannel::connect(String, event<Vrchannel*>) {
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

void Vrchannel::print_message(bool issend, const Json& message) {
    std::cout << tamer::now() << ":"
              << local_uid() << (issend ? " -> " : " <- ") << remote_uid();
    if (connection_uid())
        std::cout << " (" << connection_uid() << ")";
    std::cout << (issend ? ": send " : ": recv ") << message << "\n";
}

void Vrchannel::print_receive(const Json& message) {
    print_message(false, message);
}


// Vrtestchannel

class Vrtestchannel;
class Vrtestlistener;
class Vrtestnode;

class Vrtestcollection {
  public:
    std::vector<Vrtestnode*> nodes_;
    std::set<Vrtestchannel*> channels_;
    mutable std::mt19937 rg_;

    double rand01() const {
        std::uniform_real_distribution<double> urd;
        return urd(rg_);
    }
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

    Vrtestnode* find(const String& uid) const;
    Vrtestchannel* connect(Vrtestnode* x);

  private:
    String uid_;
    Vrtestcollection* collection_;
    Vrtestlistener* listener_;
};

class Vrtestlistener : public Vrchannel {
  public:
    inline Vrtestlistener(Vrtestnode* my_node)
        : Vrchannel(my_node->uid(), String()), my_node_(my_node) {
        set_connection_uid(my_node->uid());
    }
    void connect(String peer_uid, event<Vrchannel*> done);
    void receive_connection(event<Vrchannel*> done);
  private:
    Vrtestnode* my_node_;
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

Vrtestnode::Vrtestnode(const String& uid, Vrtestcollection* collection)
    : uid_(uid), collection_(collection) {
    collection_->nodes_.push_back(this);
    listener_ = new Vrtestlistener(this);
}

Vrtestnode* Vrtestnode::find(const String& uid) const {
    for (auto x : collection_->nodes_)
        if (x->uid() == uid)
            return x;
    return nullptr;
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

void Vrtestlistener::connect(String peer_uid, event<Vrchannel*> done) {
    if (Vrtestnode* n = my_node_->find(peer_uid))
        done(my_node_->connect(n));
    else
        done(nullptr);
}

void Vrtestlistener::receive_connection(event<Vrchannel*> done) {
    listenq_.pop_front(done);
}




tamed void go() {
    tamed {
        Vrtestcollection vrg;
        std::vector<Vrnode*> nodes;
        Vrtestnode* client;
        Vrtestchannel* client_conn;
        Json j;
    }
    for (int i = 0; i < 5; ++i)
        new Vrtestnode(Vrchannel::make_replica_uid(), &vrg);
    for (int i = 0; i < 5; ++i)
        nodes.push_back(new Vrnode(vrg.nodes_[i]->uid(),
                                   vrg.nodes_[i]->listener(),
                                   vrg.rg_));
    for (int i = 0; i < 5; ++i)
        nodes[i]->dump(std::cout);
    twait { nodes[0]->join(vrg.nodes_[1]->uid(), make_event()); }
    for (int i = 0; i < 5; ++i)
        nodes[i]->dump(std::cout);
    twait {
        nodes[0]->at_view(1, make_event());
        nodes[1]->at_view(1, make_event());
    }

    for (int i = 0; i < 5; ++i)
        nodes[i]->dump(std::cout);
    twait { nodes[2]->join(vrg.nodes_[0]->uid(), make_event()); }
    twait {
        nodes[0]->at_view(2, make_event());
        nodes[1]->at_view(2, make_event());
        nodes[2]->at_view(2, make_event());
    }

    for (int i = 0; i < 5; ++i)
        nodes[i]->dump(std::cout);
    twait { nodes[4]->join(vrg.nodes_[0]->uid(), make_event()); }
    twait {
        nodes[0]->at_view(3, make_event());
        nodes[1]->at_view(3, make_event());
        nodes[2]->at_view(3, make_event());
        nodes[4]->at_view(3, make_event());
    }
    for (int i = 0; i < 5; ++i)
        nodes[i]->dump(std::cout);

    client = new Vrtestnode(Vrchannel::make_client_uid(), &vrg);
    client_conn = client->connect(vrg.nodes_[4]);
    client_conn->send(Json::array((int) m_vri_handshake, Json::null, random_string()));
    twait { client_conn->receive(make_event(j)); }
    client_conn->send(Json::array((int) m_vri_request,
                                  Json::null,
                                  1, "req"));
    twait { client_conn->receive(make_event(j)); }
    client_conn->print_receive(j);
    twait { tamer::at_delay_usec(10000, make_event()); }
    twait { tamer::at_delay_sec(3, make_event()); }
    nodes[4]->stop();
    twait { tamer::at_delay_sec(5, make_event()); }
    nodes[4]->go();
    twait { tamer::at_delay_sec(5, make_event()); }
}

int main(int, char**) {
    tamer::initialize();
    go();
    tamer::loop();
    tamer::cleanup();
}
