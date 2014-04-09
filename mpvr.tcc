// -*- mode: c++ -*-
#include "clp.h"
#include "mpfd.hh"
#include "mpvr.hh"
#include <netdb.h>
#include <fcntl.h>
#include <unistd.h>
#include <algorithm>
#include <set>
#include <fstream>
#include <tamer/channel.hh>

static const String m_vri_request("req");
    // seqno, request [, request]*
static const String m_vri_response("res");
    // [seqno, reply]*
static const String m_vri_commit("commit");
    // P->R: [3, xxx, viewno, commitno, nundecided,
    //        [logno, [client_uid, client_seqno, request]*]]
static const String m_vri_ack("ack");
    // R->P: [3, xxx, viewno, storeno]
static const String m_vri_handshake("handshake");
    // handshake_value
static const String m_vri_join("join");
    // []
static const String m_vri_view("view");
    // view_object
static const String m_vri_error("error");

Logger logger(std::cout);

Vrconstants vrconstants;

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
            Json handshake_msg = Json::array(m_vri_handshake, Json::null,
                                             peer->connection_uid());
            log_send(peer) << handshake_msg << "\n";
            peer->send(handshake_msg);
        }
        twait {
            peer->receive(tamer::add_timeout(message_timeout,
                                             make_event(msg),
                                             Json(false)));
        }
        if (!msg.is_bool() || tamer::drecent() >= start_time + timeout)
            break;
    }

    // test handshake message
    if (!msg) { // null or false
        log_receive(peer) << "handshake timeout\n";
        done(false);
    } else if (!(msg.is_a() && msg.size() >= 3 && msg[0] == m_vri_handshake
                 && msg[2].is_s())) {
        log_receive(peer) << "bad handshake " << msg << "\n";
        done(false);
    } else {
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
    v.members[0].has_storeno_ = true;
    v.members[0].storeno_ = 0;
    v.members[0].store_count_ = 1;
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

void Vrview::prepare(String uid, const Json& payload, bool is_next) {
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
        if (!payload["commitno"].is_null() && is_next)
            account_commit(it.operator->(), payload["commitno"].to_u());
    }
}

void Vrview::clear_preparation(bool is_next) {
    nacked = nconfirmed = 0;
    for (auto& it : members)
        it.acked = it.confirmed = false;
    if (is_next)
        for (auto& it : members)
            it.has_storeno_ = false;
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
    clear_preparation(true);
    ++viewno;
    if (!viewno)
        ++viewno;
    primary_index = viewno % members.size();
}

Json Vrview::commits_json() const {
    Json j = Json::array();
    for (auto it = members.begin(); it != members.end(); ++it) {
        Json x = Json::array(it->uid);
        if (it->has_storeno_)
            x.push_back_list(it->storeno_.value(), it->store_count_);
        bool is_primary = it - members.begin() == primary_index;
        bool is_me = it - members.begin() == my_index;
        if (is_primary || is_me)
            x.push_back(String(is_primary ? "p" : "") + String(is_me ? "*" : ""));
        j.push_back(x);
    }
    return j;
}

void Vrview::account_commit(member_type* peer, lognumber_t storeno) {
    bool has_old_storeno = peer->has_storeno();
    lognumber_t old_storeno = peer->storeno();
    assert(!has_old_storeno || old_storeno <= storeno);
    peer->has_storeno_ = true;
    peer->storeno_ = storeno;
    peer->store_count_ = 0;
    if (!has_old_storeno || old_storeno != storeno)
        peer->store_changed_at_ = tamer::drecent();
    for (auto it = members.begin(); it != members.end(); ++it)
        if (it->has_storeno_) {
            if (it->storeno_ <= storeno
                && (!has_old_storeno || it->storeno_ > old_storeno)
                && &*it != peer)
                ++it->store_count_;
            if (storeno <= it->storeno_)
                ++peer->store_count_;
        }
}

bool Vrview::account_all_commits() {
    bool changed = false;
    Json cj = commits_json();
    for (auto it = members.begin(); it != members.end(); ++it) {
        unsigned old_store_count = it->store_count_;
        it->store_count_ = 0;
        for (auto jt = members.begin(); jt != members.end(); ++jt)
            if (it->has_storeno_ && jt->has_storeno_
                && it->storeno_ <= jt->storeno_)
                ++it->store_count_;
        changed = changed || it->store_count_ != old_store_count;
    }
#if 0
    std::cerr << (changed ? "! " : ". ") << " => " << cj << " => " << commits_json() << "\n";
#endif
    return changed;
}



Vrreplica::Vrreplica(const String& group_name, Vrchannel* me, std::mt19937& rg)
    : group_name_(group_name), want_member_(!!me), me_(me),
      first_logno_(0), commitno_(0), decideno_(0), commitno_sticky_(false),
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

void Vrreplica::dump(std::ostream& out) const {
    timeval now = tamer::now();
    out << now << ":" << uid() << ": " << unparse_view_state()
        << " " << cur_view_.members_json()
        << " p@" << cur_view_.primary_index << "\n";
}

String Vrreplica::unparse_view_state() const {
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
        sa << first_logno() << ":";
        if (decideno() != first_logno())
            sa << decideno();
        sa << ":";
        if (commitno() != decideno() || commitno_sticky_)
            sa << commitno();
        if (commitno_sticky_)
            sa << "*";
        sa << ":";
        if (last_logno() != commitno())
            sa << last_logno();
    }
    return sa.take_string();
}

tamed void Vrreplica::listen_loop() {
    tamed { Vrchannel* peer; }
    while (1) {
        twait { me_->receive_connection(make_event(peer)); }
        if (!peer)
            break;
        connection_handshake(peer, false);
    }
}

tamed void Vrreplica::connect(String peer_uid, event<> done) {
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

    log_connection(uid(), peer_uid) << "connecting\n";
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

tamed void Vrreplica::join(String peer_uid, event<> done) {
    tamed { Vrchannel* ep; }
    assert(want_member_ && next_view_.size() == 1);
    while (next_view_.size() == 1) {
        if ((ep = endpoints_[peer_uid])) {
            ep->send(Json::array(m_vri_join, Json::null));
            twait {
                at_view(next_view_.viewno + 1,
                        tamer::add_timeout(k_.message_timeout, make_event()));
            }
        } else
            twait { connect(peer_uid, make_event()); }
    }
    done();
}

void Vrreplica::join(String peer_uid, Json peer_name, event<> done) {
    node_names_[peer_uid] = std::move(peer_name);
    return join(peer_uid, done);
}

tamed void Vrreplica::connection_handshake(Vrchannel* peer, bool active_end) {
    tamed { bool ok = false; }

    twait {
        handshake_protocol(peer, active_end, k_.message_timeout,
                           k_.handshake_timeout, make_event(ok));
    }

    String peer_uid = peer->remote_uid();

    if (ok && endpoints_[peer_uid]) {
        String old_cuid = endpoints_[peer_uid]->connection_uid();
        if (old_cuid < peer->connection_uid())
            log_connection(peer) << "preferring old connection (" << old_cuid << ")\n";
        else {
            log_connection(peer) << "dropping old connection (" << old_cuid << ")\n";
            endpoints_[peer_uid]->close();
            endpoints_[peer_uid] = peer;
        }
    } else if (ok)
        endpoints_[peer_uid] = peer;

    connection_wait_[peer_uid]();
    connection_wait_.erase(peer_uid);
    if (ok)
        connection_loop(peer);
    else
        delete peer;
}

tamed void Vrreplica::connection_loop(Vrchannel* peer) {
    tamed { Json msg; }

    while (1) {
        msg.clear();
        twait { peer->receive(make_event(msg)); }
        if (!msg || !msg.is_a() || msg.size() < 2)
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
        else if (msg[0] == m_vri_ack)
            process_ack(peer, msg);
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

void Vrreplica::at_view(viewnumber_t viewno, tamer::event<> done) {
    if (viewno > cur_view_.viewno)
        at_view_.push_back(std::make_pair(viewno, std::move(done)));
    else
        done();
}

void Vrreplica::at_store(lognumber_t storeno, tamer::event<> done) {
    if (storeno > last_logno())
        at_store_.push_back(std::make_pair(storeno, std::move(done)));
    else
        done();
}

void Vrreplica::at_commit(viewnumber_t commitno, tamer::event<> done) {
    if (commitno > commitno_)
        at_commit_.push_back(std::make_pair(commitno, std::move(done)));
    else
        done();
}

void Vrreplica::process_view(Vrchannel* who, const Json& msg) {
    Json payload = msg[2];
    Vrview v;
    if (!v.assign(payload, uid())
        || !v.count(who->remote_uid())) {
        who->send(Json::array(m_vri_error, -msg[1]));
        return;
    }

    viewnumberdiff_t vdiff = (viewnumberdiff_t) (v.viewno - next_view_.viewno);
    int want_send;
    if (vdiff < 0
        || (vdiff == 0 && v != next_view_)
        || !next_view_.shared_quorum(v))
        want_send = 2;
    else if (vdiff == 0) {
        cur_view_.prepare(who->remote_uid(), payload, false);
        next_view_.prepare(who->remote_uid(), payload, true);
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
        cur_view_.prepare(who->remote_uid(), payload, false);
        next_view_.prepare(who->remote_uid(), payload, true);
        broadcast_view();
        want_send = 0;
    }

    if (cur_view_.nacked > cur_view_.f()
        && next_view_.nacked > next_view_.f()
        && (next_view_.me_primary()
            || next_view_.primary().acked)
        && !next_view_sent_confirm_) {
        if (next_view_.me_primary())
            next_view_.prepare(uid(), Json::object("confirm", true), true);
        else
            send_view(next_view_.primary().uid);
        next_view_sent_confirm_ = true;
    }
    if (next_view_.nconfirmed > next_view_.f()
        && next_view_.me_primary()
        && want_send != 2) {
        if (cur_view_.viewno != next_view_.viewno) {
            next_view_.account_all_commits();
            cur_view_ = next_view_;
            commitno_sticky_ = false;
            process_at_number(cur_view_.viewno, at_view_);
            primary_keepalive_loop();
            log_connection(who) << uid() << " adopts view " << unparse_view_state() << "\n";
            for (auto it = cur_view_.members.begin();
                 it != cur_view_.members.end(); ++it)
                if (it->confirmed)
                    send_commit_log(&*it, it->storeno());
        } else
            send_commit_log(cur_view_.find_pointer(who->remote_uid()), commitno());
    } else if (want_send)
        send_view(who);
}

void Vrreplica::process_view_log_transfer(Json& payload) {
    assert(payload["logno"].is_u()
           && payload["log"].is_a()
           && payload["log"].size() % 4 == 0);
    lognumber_t logno = payload["logno"].to_u();
    const Json& log = payload["log"];
    for (int i = 0; i != log.size(); i += 4, ++logno)
        if (logno >= first_logno_) {
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

Json Vrreplica::view_payload(const String& peer_uid) {
    Json payload = Json::object("viewno", next_view_.viewno.value(),
                                "members", next_view_.members_json(),
                                "primary", next_view_.primary_index,
                                "commitno", commitno_.value());
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
        && next_view_.primary().has_storeno()
        && peer_uid == next_view_.primary().uid) {
        lognumber_t logno = std::max(first_logno_, next_view_.primary().storeno());
        payload["logno"] = logno.value();
        Json log = Json::array();
        for (; logno < last_logno(); ++logno) {
            auto& li = log_[logno - first_logno_];
            log.push_back_list(li.viewno.value(),
                               li.client_uid,
                               li.client_seqno,
                               li.request);
        }
        payload["log"] = std::move(log);
    }
    return payload;
}

void Vrreplica::send_view(Vrchannel* who, Json payload, Json seqno) {
    if (!payload.get("members"))
        payload.merge(view_payload(who->remote_uid()));
    Json msg = Json::array(m_vri_view, seqno, payload);
    who->send(msg);
    log_send(who) << msg << " " << unparse_view_state() << "\n";
}

tamed void Vrreplica::send_view(String peer_uid) {
    tamed { Json payload; Vrchannel* ep; }
    payload = view_payload(peer_uid);
    while (!(ep = endpoints_[peer_uid]))
        twait { connect(peer_uid, make_event()); }
    if (ep != me_)
        send_view(ep, payload);
}

void Vrreplica::broadcast_view() {
    for (auto it = next_view_.members.begin();
         it != next_view_.members.end(); ++it)
        send_view(it->uid);
}

void Vrreplica::process_join(Vrchannel* who, const Json&) {
    Vrview v;
    if (!next_view_.count(who->remote_uid())) {
        next_view_.add(who->remote_uid(), uid());
        start_view_change();
    }
}

void Vrreplica::initialize_next_view() {
    cur_view_.clear_preparation(false);
    next_view_sent_confirm_ = false;
    Json my_msg = Json::object("commitno", last_logno().value());
    cur_view_.prepare(uid(), my_msg, false);
    next_view_.prepare(uid(), my_msg, true);
}

tamed void Vrreplica::start_view_change() {
    tamed { viewnumber_t view = next_view_.viewno; }
    initialize_next_view();
    broadcast_view();

    // kick off another view change if this one appears to fail
    twait { tamer::at_delay(k_.view_change_timeout * (1 + rand01() / 8),
                            make_event()); }
    if (cur_view_.viewno < view) {
        logger() << tamer::recent() << ":" << uid() << ": timing out view "
                 << unparse_view_state() << "\n";
        next_view_.advance();
        start_view_change();
    }
}

void Vrreplica::process_request(Vrchannel* who, const Json& msg) {
    if (msg.size() < 4 || !msg[2].is_i())
        who->send(Json::array(m_vri_error, msg[1], false));
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
        cur_view_.account_commit(&cur_view_.primary(), last_logno());
    }
}

Json Vrreplica::commit_log_message(lognumber_t first) const {
    Json msg = Json::array(m_vri_commit,
                           Json::null,
                           cur_view_.viewno.value(),
                           commitno_.value(),
                           commitno_ - decideno_);
    first = std::max(first, first_logno_);
    if (first < last_logno()) {
        msg.reserve(msg.size() + 1 + (last_logno() - first) * 4);
        msg.push_back(first.value());
        for (lognumber_t i = first; i != last_logno(); ++i) {
            const log_item& li = log_[i - first_logno_];
            msg.push_back_list(cur_view_.viewno - li.viewno,
                               li.client_uid, li.client_seqno, li.request);
        }
    }
    return msg;
}

tamed void Vrreplica::send_peer(String peer_uid, Json msg) {
    tamed { Vrchannel* ep = nullptr; }
    while (!(ep = endpoints_[peer_uid]))
        twait { connect(peer_uid, make_event()); }
    if (ep != me_)
        ep->send(msg);
}

void Vrreplica::send_commit_log(Vrview::member_type* peer, lognumber_t first) {
    if (peer->has_storeno() && peer->storeno() < first)
        first = peer->storeno();
    send_peer(peer->uid, commit_log_message(first));
}

void Vrreplica::broadcast_commit(lognumber_t from_storeno) {
    assert(is_primary());
    Json msg = commit_log_message(from_storeno);
    for (auto it = cur_view_.members.begin();
         it != cur_view_.members.end(); ++it)
        if ((it->has_storeno() && it->storeno() == from_storeno)
            || tamer::drecent() <=
                 it->store_changed_at() + k_.retransmit_log_timeout)
            send_peer(it->uid, msg);
        else
            send_commit_log(&*it, it->storeno());
    commit_sent_at_ = tamer::drecent();
}

void Vrreplica::process_commit(Vrchannel* who, const Json& msg) {
    if (msg.size() < 5
        || (msg.size() > 5 && (msg.size() - 6) % 4 != 0)
        || !msg[2].is_u()
        || !msg[3].is_u()
        || (msg.size() > 4 && !msg[4].is_u())) {
        who->send(Json::array(m_vri_error, msg[1], false));
        return;
    }

    viewnumber_t view(msg[2].to_u());
    if (view == next_view_.viewno
        && cur_view_.viewno != next_view_.viewno) {
        assert(!next_view_.me_primary()
               && next_view_.primary().uid == who->remote_uid());
        cur_view_ = next_view_;
        next_view_sent_confirm_ = true;
        commitno_sticky_ = true; // acknowledge `commitno_` until log confirmed
        process_at_number(cur_view_.viewno, at_view_);
        backup_keepalive_loop();
    } else if (view != cur_view_.viewno || between_views()) {
        send_view(who);
        return;
    }

    lognumber_t commitno = msg[3].to_u();
    lognumber_t decideno = commitno - msg[4].to_u();
    // decideno indicates that all replicas, including us, agree. Use it to
    // advance commitno_. (Retransmitted commits won't work before decideno,
    // because others may have truncated their logs.)
    // NB might have decideno < first_logno() near view changes!
    assert(decideno <= last_logno());
    commitno_ = std::max(commitno_, decideno);

    if (msg.size() > 6)
        process_commit_log(msg);

    if (commitno > commitno_
        && commitno <= last_logno()
        && !commitno_sticky_) {
        commitno_ = commitno;
        process_at_number(commitno_, at_commit_);
    }

    if (decideno > decideno_ && decideno <= commitno_) {
        decideno_ = decideno;
        while (first_logno_ < decideno_) {
            log_.pop_front();
            ++first_logno_;
        }
    }

    who->send(Json::array(m_vri_ack, Json::null, cur_view_.viewno.value(),
                          commitno_sticky_ ? commitno_.value() : last_logno().value()));

    primary_received_at_ = tamer::drecent();
}

void Vrreplica::process_commit_log(const Json& msg) {
    lognumber_t logno = msg[5].to_u();
    size_t nlog = (msg.size() - 6) / 4;

    if (commitno_sticky_) {
        if (logno <= commitno_
            && logno + nlog < last_logno()
            && log_[(logno + nlog) - first_logno_].viewno == cur_view_.viewno)
            // can preserve later items in log, they're from this view
            commitno_sticky_ = false;
        else if (logno <= commitno_) {
            log_.resize(commitno_ - first_logno_);
            commitno_sticky_ = false;
        } else
            while (last_logno() < logno)
                log_.emplace_back(cur_view_.viewno - 1, String(), 0, Json());
    }

    for (int i = 6; i != msg.size(); i += 4, ++logno) {
        size_t logpos = logno - first_logno_;
        if (commitno_ <= logno && logpos <= log_.size()) {
            log_item li(cur_view_.viewno - msg[i].to_u(),
                        msg[i + 1].to_s(), msg[i + 2].to_u(), msg[i + 3]);
            if (logpos == log_.size())
                log_.push_back(std::move(li));
            else if (log_[logpos].viewno < cur_view_.viewno)
                log_[logpos] = std::move(li);
        }
    }

    process_at_number(last_logno(), at_store_);
}

void Vrreplica::update_commitno(lognumber_t commitno) {
    std::unordered_map<String, Json> messages;
    for (size_t i = commitno_.value(); i != commitno.value(); ++i) {
        log_item& li = log_[i - first_logno_.value()];
        Json& msg = messages[li.client_uid];
        if (!msg)
            msg = Json::array(m_vri_response, Json::null);
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

void Vrreplica::process_ack(Vrchannel* who, const Json& msg) {
    if (msg.size() < 4
        || !msg[2].is_u()
        || !msg[3].is_u()) {
        who->send(Json::array(m_vri_error, msg[1], false));
        return;
    }

    viewnumber_t view(msg[2].to_u());
    Vrview::member_type* peer = cur_view_.find_pointer(who->remote_uid());
    if (view != cur_view_.viewno || between_views() || !peer) {
        send_view(who);
        return;
    }

    lognumber_t commitno = msg[3].to_u();
    cur_view_.account_commit(peer, commitno);
    assert(!cur_view_.account_all_commits());
    if (peer->store_count() > cur_view_.f()
        && commitno > commitno_)
        update_commitno(commitno);
    if (peer->store_count() == cur_view_.size()
        && commitno > decideno_)
        decideno_ = commitno;
    while (first_logno_ < decideno_) {
        log_.pop_front();
        ++first_logno_;
    }
}

tamed void Vrreplica::primary_keepalive_loop() {
    tamed { viewnumber_t view = cur_view_.viewno; }
    while (1) {
        twait { tamer::at_delay(k_.primary_keepalive_timeout / 4,
                                make_event()); }
        if (!in_view(view))
            break;
        if (tamer::drecent() - commit_sent_at_
              >= k_.primary_keepalive_timeout / 2
            && !stopped_) {
            for (auto it = cur_view_.members.begin();
                 it != cur_view_.members.end(); ++it)
                send_commit_log(&*it, it->storeno());
            commit_sent_at_ = tamer::drecent();
        }
    }
}

tamed void Vrreplica::backup_keepalive_loop() {
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

void Vrreplica::stop() {
    stopped_ = true;
}

void Vrreplica::go() {
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
            channel_->send(Json::array(m_vri_request,
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
        msg.clear();
        twait { peer->receive(make_event(msg)); }
        if (!msg || !msg.is_a() || msg.size() < 2)
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
    if (peer == channel_)
        channel_ = nullptr;
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
    if (view_.assign(msg[2], String())
        && (!channel_ || view_.primary().uid != channel_->remote_uid())) {
        if (channel_)
            channel_->close();
        channel_ = nullptr;
        connect(view_.primary().uid, view_.primary().peer_name, event<>());
    }
}

tamed void Vrclient::connect(String peer_uid, Json peer_name, event<> done) {
    tamed { Vrchannel* peer; bool ok; int tries = 0; }
    while (1) {
        peer = nullptr;
        ok = false;

        twait { me_->connect(peer_uid, peer_name, make_event(peer)); }

        if (peer) {
            peer->set_connection_uid(random_string(rg_));
            twait { handshake_protocol(peer, true, vrconstants.message_timeout,
                                       10000, make_event(ok)); }
        }

        if (peer && ok) {
            channel_ = peer;
            connection_loop(peer);
            done();
            return;
        }

        delete peer;
        // every 8th try, look for someone else
        ++tries;
        if (tries % 8 == 7 && view_.size()) {
            unsigned i = std::uniform_int_distribution<unsigned>(0, view_.size() - 1)(rg_);
            peer_uid = view_.members[i].uid;
            peer_name = view_.members[i].peer_name;
        }
    }
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

    Vrtestcollection(unsigned seed, double loss_p);

    Vrreplica* add_replica(const String& uid);
    Vrclient* add_client(const String& uid);

    inline unsigned size() const {
        return replicas_.size();
    }
    inline unsigned f() const {
        return replicas_.size() / 2;
    }
    inline double loss_p() const {
        return loss_p_;
    }

    Vrtestnode* test_node(const String& s) const {
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
    double loss_p_;

    std::unordered_map<String, Vrreplica*> replica_map_;
    std::vector<Vrreplica*> replicas_;
    std::unordered_map<String, Vrtestnode*> testnodes_;

    lognumber_t first_logno_;
    std::deque<Vrreplica::log_item> committed_log_;
    lognumber_t decideno_;
    lognumber_t commitno_;

    void print_lognos() const;
    void print_log_position(lognumber_t l) const;
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

Vrreplica* Vrtestcollection::add_replica(const String& uid) {
    assert(testnodes_.find(uid) == testnodes_.end());
    Vrtestnode* tn = new Vrtestnode(uid, this);
    testnodes_[uid] = tn;
    Vrreplica* r = new Vrreplica(tn->uid(), tn->listener(), rg_);
    replica_map_[uid] = r;
    replicas_.push_back(r);
    std::sort(replicas_.begin(), replicas_.end(), [](Vrreplica* a, Vrreplica* b) {
            return a->uid() < b->uid();
        });
    return r;
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
      delay_(0.05 + 0.0125 * from->collection()->rand01()),
      loss_p_(from->collection()->loss_p()) {
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
    if (peer_) {
        peer_->do_send(Json());
        peer_->peer_ = 0;
    }
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
    if (Vrtestnode* n = collection_->test_node(peer_uid))
        done(collection_->test_node(local_uid())->connect(n));
    else
        done(nullptr);
}

void Vrtestlistener::receive_connection(event<Vrchannel*> done) {
    listenq_.pop_front(done);
}


Vrtestcollection::Vrtestcollection(unsigned seed, double loss_p)
    : rg_(seed), loss_p_(loss_p), first_logno_(0), decideno_(0),
      commitno_(0) {
}

void Vrtestcollection::print_lognos() const {
    const char* sep = "  ";
    for (auto r : replicas_) {
        std::cerr << sep << r->uid() << ":" << r->first_logno() << ":";
        if (r->decideno() != r->first_logno())
            std::cerr << r->decideno();
        std::cerr << ":";
        if (r->commitno() != r->decideno())
            std::cerr << r->commitno();
        std::cerr << ":";
        if (r->last_logno() != r->commitno())
            std::cerr << r->last_logno();
        std::cerr << "(" << r->current_view().commits_json() << ")";
        sep = ", ";
    }
    std::cerr << "\n";
}

void Vrtestcollection::print_log_position(lognumber_t l) const {
    std::cerr << "  l#" << l << "<";
    const char* sep = "";
    for (auto r : replicas_)
        if (l < r->last_logno()) {
            std::cerr << sep << r->uid() << ":";
            if (l < r->first_logno())
                std::cerr << "trunc";
            else {
                const Vrreplica::log_item& li = r->log_entry(l);
                std::cerr << li.request << "@v#" << li.viewno;
            }
            sep = ", ";
        }
    std::cerr << ">\n";
}

void Vrtestcollection::check() {
    unsigned f = this->f();

    // calculate actual commit numbers
    std::vector<lognumber_t> first_lognos, last_lognos;
    lognumber_t max_decideno, max_commitno;
    for (auto r : replicas_) {
        first_lognos.push_back(r->first_logno());
        last_lognos.push_back(r->last_logno());
        if (r == replicas_.front()) {
            max_decideno = r->decideno();
            max_commitno = r->commitno();
        } else {
            max_decideno = std::max(max_decideno, r->decideno());
            max_commitno = std::max(max_commitno, r->commitno());
        }
    }
    std::sort(first_lognos.begin(), first_lognos.end());
    lognumber_t first_logno = first_lognos[f];
    std::sort(last_lognos.begin(), last_lognos.end());
    lognumber_t last_logno = last_lognos.back();

    // commit never goes backwards
    assert(max_decideno >= decideno_);
    decideno_ = max_decideno;

    // advance commit number (check replication)
    std::vector<unsigned> commitmap;
    std::vector<const Vrreplica::log_item*> itemmap;
    while (1) {
        itemmap.assign(replicas_.size(), nullptr);
        commitmap.assign(replicas_.size(), 1);
        for (size_t i = 0; i != replicas_.size(); ++i) {
            Vrreplica* r = replicas_[i];
            if (commitno_ >= r->first_logno() && commitno_ < r->last_logno()
                && r->log_entry(commitno_).is_real())
                itemmap[i] = &r->log_entry(commitno_);
        }
        for (size_t i = 1; i != replicas_.size(); ++i)
            if (itemmap[i])
                for (size_t j = 0; j != i; ++j)
                    if (itemmap[j] && itemmap[i]->viewno == itemmap[j]->viewno) {
                        ++commitmap[j];
                        break;
                    }
        auto maxindex = std::max_element(commitmap.begin(), commitmap.end()) - commitmap.begin();
        if (commitmap[maxindex] <= f)
            break;
        committed_log_.push_back(replicas_[maxindex]->log_entry(commitno_));
        ++commitno_;
    }
    // no one is allowed to think more has committed than has actually committed
    assert(max_commitno <= commitno_);

    // count # committed
    std::vector<unsigned> commit_counts(last_logno - first_logno, 0);

    // check integrity of log
    for (auto r : replicas_) {
        assert(commitno_ >= r->commitno());
        assert(max_decideno >= r->decideno());
        if (r->first_logno() > r->decideno()
            || r->decideno() > r->commitno()
            || r->commitno() > r->last_logno())
            std::cerr << "check: " << r->uid() << " bad commits "
                      << r->first_logno() << ":" << r->decideno()
                      << ":" << r->commitno() << ":" << r->last_logno() << "\n";
        assert(r->first_logno() <= r->decideno());
        assert(r->decideno() <= r->commitno());
        assert(r->commitno() <= r->last_logno());
        lognumber_t first = std::max(first_logno, r->first_logno());
        lognumber_t last = std::min(commitno_, r->last_logno());
        for (; first < last; ++first) {
            const Vrreplica::log_item& li = r->log_entry(first);
            const Vrreplica::log_item& cli = committed_log_[first - first_logno_];
            if (li.is_real()) {
                assert(cli.viewno != li.viewno || cli == li);
                if (first < first_logno + commit_counts.size() && cli.viewno == li.viewno)
                    ++commit_counts[first - first_logno];
            }
        }
    }

    // Every "decided" log element has all commits
    unsigned truncatepos = 0, missingpos = 0;
    for (lognumber_t l = first_logno; l != max_decideno; ++l) {
        while (truncatepos != size() && first_lognos[truncatepos] <= l)
            ++truncatepos;
        while (missingpos != size() && last_lognos[missingpos] <= l)
            ++missingpos;
        if (commit_counts[l - first_logno] != truncatepos - missingpos) {
            std::cerr << "check: decided l#" << l << "<" << committed_log_[l - first_logno_].request << "> replicated only " << commit_counts[l - first_logno] << " times (want " << (truncatepos - missingpos) << ")\n";
            print_lognos();
            print_log_position(l);
        }
        assert(commit_counts[l - first_logno] == truncatepos - missingpos);
    }
    // Every "committed" log element has >= f + 1 commits
    for (lognumber_t l = max_decideno; l != max_commitno; ++l) {
        if (commit_counts[l - first_logno] < f + 1) {
            std::cerr << "check: committed l#" << l << "<" << committed_log_[l - first_logno_].request << "> replicated only " << commit_counts[l - first_logno] << " times\n";
            print_lognos();
            print_log_position(l);
        }
        assert(commit_counts[l - first_logno] >= f + 1);
    }
}


tamed void many_requests(Vrclient* client) {
    tamed { int n = 1; }
    while (1) {
        twait { client->request("req" + String(n), make_event()); }
        ++n;
        twait { tamer::at_delay(0.5, make_event()); }
    }
}

tamed void go(Vrtestcollection& vrg, std::vector<Vrreplica*>& nodes) {
    tamed {
        Vrclient* client;
        Json j;
    }
    for (unsigned i = 0; i < nodes.size(); ++i)
        nodes[i]->dump(std::cout);
    twait { nodes[0]->join(nodes[1]->uid(), make_event()); }
    for (unsigned i = 0; i < nodes.size(); ++i)
        nodes[i]->dump(std::cout);
    twait {
        nodes[0]->at_view(1, make_event());
        nodes[1]->at_view(1, make_event());
    }

    for (unsigned i = 0; i < nodes.size(); ++i)
        nodes[i]->dump(std::cout);
    twait { nodes[2]->join(nodes[0]->uid(), make_event()); }
    twait {
        nodes[0]->at_view(2, make_event());
        nodes[1]->at_view(2, make_event());
        nodes[2]->at_view(2, make_event());
    }

    for (unsigned i = 0; i < nodes.size(); ++i)
        nodes[i]->dump(std::cout);
    twait { nodes[4]->join(nodes[0]->uid(), make_event()); }
    twait {
        nodes[0]->at_view(3, make_event());
        nodes[1]->at_view(3, make_event());
        nodes[2]->at_view(3, make_event());
        nodes[4]->at_view(3, make_event());
    }
    for (unsigned i = 0; i < nodes.size(); ++i)
        nodes[i]->dump(std::cout);

    client = vrg.add_client(Vrchannel::make_client_uid());
    twait { client->connect(nodes[0]->uid(), make_event()); }
    many_requests(client);
    twait { tamer::at_delay_usec(10000, make_event()); }
    twait { tamer::at_delay_sec(3, make_event()); }
    nodes[4]->stop();
    twait { tamer::at_delay_sec(5, make_event()); }
    nodes[4]->go();

    twait { tamer::at_delay_sec(50000, make_event()); }
    exit(0);
}

static Clp_Option options[] = {
    { "f", 'f', 0, Clp_ValUnsigned, 0 },
    { "loss", 'l', 0, Clp_ValDouble, 0 },
    { "n", 'n', 0, Clp_ValUnsigned, 0 },
    { "quiet", 'q', 0, 0, Clp_Negate },
    { "seed", 's', 0, Clp_ValUnsigned, 0 }
};

int main(int argc, char** argv) {
    Clp_Parser* clp = Clp_NewParser(argc, argv, sizeof(options)/sizeof(options[0]), options);
    unsigned n = 0;
    unsigned seed = std::mt19937::default_seed;
    double loss_p = 0.1;
    while (Clp_Next(clp) != Clp_Done) {
        if (Clp_IsLong(clp, "seed"))
            seed = clp->val.u;
        else if (Clp_IsLong(clp, "f")) {
            assert(n == 0);
            n = 2 * clp->val.u + 1;
        } else if (Clp_IsLong(clp, "n")) {
            assert(n == 0);
            n = clp->val.u;
        } else if (Clp_IsLong(clp, "loss")) {
            assert(clp->val.d >= 0 && clp->val.d <= 1);
            loss_p = clp->val.d;
        } else if (Clp_IsLong(clp, "quiet"))
            logger.set_frequency(clp->negated ? 0 : 2000);
    }
    n = n ? n : 5;

    tamer::set_time_type(tamer::time_virtual);
    tamer::initialize();

    Vrtestcollection vrg(seed, loss_p);
    std::vector<Vrreplica*> nodes;
    for (unsigned i = 0; i < n; ++i)
        nodes.push_back(vrg.add_replica(Vrchannel::make_replica_uid()));

    go(vrg, nodes);

    while (1) {
        tamer::once();
        vrg.check();
    }

    tamer::cleanup();
}
