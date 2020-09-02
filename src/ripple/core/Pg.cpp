//------------------------------------------------------------------------------
/*
    This file is part of rippled: https://github.com/ripple/rippled
    Copyright (c) 2020 Ripple Labs Inc.

    Permission to use, copy, modify, and/or distribute this software for any
    purpose  with  or without fee is hereby granted, provided that the above
    copyright notice and this permission notice appear in all copies.

    THE  SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
    WITH  REGARD  TO  THIS  SOFTWARE  INCLUDING  ALL  IMPLIED  WARRANTIES  OF
    MERCHANTABILITY  AND  FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
    ANY  SPECIAL ,  DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
    WHATSOEVER  RESULTING  FROM  LOSS  OF USE, DATA OR PROFITS, WHETHER IN AN
    ACTION  OF  CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
    OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
*/
//==============================================================================

#include <ripple/basics/Slice.h>
#include <ripple/protocol/digest.h>
#include <ripple/basics/contract.h>
#include <ripple/basics/strHex.h>
#include <ripple/basics/StringUtilities.h>
#include <ripple/beast/core/CurrentThreadName.h>
#include <ripple/core/Pg.h>
#include <ripple/nodestore/impl/codec.h>
#include <ripple/nodestore/impl/DecodedBlob.h>
#include <ripple/nodestore/impl/EncodedBlob.h>
#include <ripple/nodestore/NodeObject.h>
#include <nudb/nudb.hpp>
#include <boost/asio/error.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/strand.hpp>
#include <boost/format.hpp>
#include <algorithm>
#include <cassert>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <functional>
#include <iterator>
#include <stdexcept>
#include <sstream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace ripple {

static void
noticeReceiver(void* arg, PGresult const* res)
{
    beast::Journal& j = *static_cast<beast::Journal*>(arg);
    JLOG(j.error()) << "server message: "
                         << PQresultErrorMessage(res);
}

/*
 Connecting described in:
 https://www.postgresql.org/docs/10/libpq-connect.html
 */
void
Pg::connect()
{
    if (conn_)
    {
        // Nothing to do if we already have a good connection.
        if (PQstatus(conn_.get()) == CONNECTION_OK)
            return;
        /* Try resetting connection. */
        PQreset(conn_.get());
    }
    else  // Make new connection.
    {
        conn_.reset(PQconnectdbParams(
            reinterpret_cast<char const* const*>(&config_.keywordsIdx[0]),
            reinterpret_cast<char const* const*>(&config_.valuesIdx[0]),
            0));
        if (!conn_)
            Throw<std::runtime_error>("No db connection struct");
    }

    /** Results from a synchronous connection attempt can only be either
     * CONNECTION_OK or CONNECTION_BAD. */
    if (PQstatus(conn_.get()) == CONNECTION_BAD)
    {
        std::stringstream ss;
        ss << "DB connection status " << PQstatus(conn_.get()) << ": "
            << PQerrorMessage(conn_.get());
        Throw<std::runtime_error>(ss.str());
    }

    // Log server session console messages.
    PQsetNoticeReceiver(
        conn_.get(), noticeReceiver, const_cast<beast::Journal*>(&j_));
}

pg_variant_type
Pg::query(char const* command, std::size_t nParams, char const* const* values)
{
    pg_result_type ret { nullptr, [](PGresult* result){ PQclear(result); }};
    // Connect then submit query.
    while (true)
    {
        try
        {
            connect();
            if (nParams)
            {
                ret.reset(PQexecParams(
                    conn_.get(),
                    command,
                    nParams,
                    nullptr,
                    values,
                    nullptr,
                    nullptr,
                    0));
            }
            else
            {
                ret.reset(PQexec(conn_.get(), command));
            }
            if (!ret)
                Throw<std::runtime_error>("no result structure returned");
            break;
        }
        catch (std::exception const& e)
        {
            // Sever connection and retry until successful.
            disconnect();
            JLOG(j_.error()) << "database error, retrying: " << e.what();
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
    }

    // Ensure proper query execution.
    switch (PQresultStatus(ret.get()))
    {
        case PGRES_TUPLES_OK:
        case PGRES_COMMAND_OK:
        case PGRES_COPY_IN:
        case PGRES_COPY_OUT:
        case PGRES_COPY_BOTH:
            break;
        default:
        {
            std::stringstream ss;
            ss << "bad query result: "
               << PQresStatus(PQresultStatus(ret.get()))
               << " error message: "
               << PQerrorMessage(conn_.get())
               << ", number of tuples: "
               << PQntuples(ret.get())
               << ", number of fields: "
               << PQnfields(ret.get());
            JLOG(j_.error()) << ss.str();
            disconnect();
            return pg_error_type{
                PQresultStatus(ret.get()), PQerrorMessage(conn_.get())};
        }
    }

    return ret;
}

static pg_formatted_params
formatParams(pg_params const& dbParams, beast::Journal const j)
{
    std::vector<std::optional<std::string>> const& values = dbParams.second;
    /* Convert vector to C-style array of C-strings for postgres API.
       std::nullopt is a proxy for NULL since an empty std::string is
       0 length but not NULL. */
    std::vector<char const*> valuesIdx;
    valuesIdx.reserve(values.size());
    std::stringstream ss;
    bool first = true;
    for (auto const& value : values)
    {
        if (value)
        {
            valuesIdx.push_back(value->c_str());
            ss << value->c_str();
        }
        else
        {
            valuesIdx.push_back(nullptr);
            ss << "(null)";
        }
        if (first)
            first = false;
        else
            ss << ',';
    }

    JLOG(j.trace()) << "query: " << dbParams.first << ". params: " << ss.str();
    return valuesIdx;
}

pg_variant_type
Pg::query(pg_params const& dbParams)
{
    char const* const& command = dbParams.first;
    auto const formattedParams = formatParams(dbParams, j_);
    return query(
        command,
        formattedParams.size(),
        formattedParams.size()
            ? reinterpret_cast<char const* const*>(&formattedParams[0])
            : nullptr);
}

//-----------------------------------------------------------------------------

PgPool::PgPool(Section const& network_db_config, beast::Journal const j) : j_(j)
{
    /*
    Connect to postgres to create low level connection parameters
    with optional caching of network address info for subsequent connections.
    See https://www.postgresql.org/docs/10/libpq-connect.html

    For bounds checking of postgres connection data received from
    the network: the largest size for any connection field in
    PG source code is 64 bytes as of 5/2019. There are 29 fields.
    */
    constexpr std::size_t maxFieldSize = 1024;
    constexpr std::size_t maxFields = 1000;

    // PostgreSQL connection
    pg_connection_type conn(
        PQconnectdb(get<std::string>(network_db_config, "conninfo").c_str()),
        [](PGconn* conn){PQfinish(conn);});
    if (! conn)
        Throw<std::runtime_error>("Can't create DB connection.");
    if (PQstatus(conn.get()) != CONNECTION_OK)
    {
        std::stringstream ss;
        ss << "Initial DB connection failed: "
           << PQerrorMessage(conn.get());
        Throw<std::runtime_error>(ss.str());
    }

    int const sockfd = PQsocket(conn.get());
    if (sockfd == -1)
        Throw<std::runtime_error>("No DB socket is open.");
    struct sockaddr_storage addr;
    socklen_t len = sizeof(addr);
    if (getpeername(
        sockfd, reinterpret_cast<struct sockaddr*>(&addr), &len) == -1)
    {
        Throw<std::system_error>(errno, std::generic_category(),
            "Can't get server address info.");
    }

    // Set "port" and "hostaddr" if we're caching it.
    bool const remember_ip = get(network_db_config, "remember_ip", true);

    if (remember_ip)
    {
        config_.keywords.push_back("port");
        config_.keywords.push_back("hostaddr");
        std::string port;
        std::string hostaddr;

        if (addr.ss_family == AF_INET)
        {
            hostaddr.assign(INET_ADDRSTRLEN, '\0');
            struct sockaddr_in const &ainfo =
                reinterpret_cast<struct sockaddr_in &>(addr);
            port = std::to_string(ntohs(ainfo.sin_port));
            if (!inet_ntop(AF_INET, &ainfo.sin_addr,
                &hostaddr[0],
                hostaddr.size()))
            {
                Throw<std::system_error>(errno, std::generic_category(),
                    "Can't get IPv4 address string.");
            }
        }
        else if (addr.ss_family == AF_INET6)
        {
            hostaddr.assign(INET6_ADDRSTRLEN, '\0');
            struct sockaddr_in6 const &ainfo =
                reinterpret_cast<struct sockaddr_in6 &>(addr);
            port = std::to_string(ntohs(ainfo.sin6_port));
            if (! inet_ntop(AF_INET6, &ainfo.sin6_addr,
                &hostaddr[0],
                hostaddr.size()))
            {
                Throw<std::system_error>(errno, std::generic_category(),
                    "Can't get IPv6 address string.");
            }
        }

        config_.values.push_back(port.c_str());
        config_.values.push_back(hostaddr.c_str());
    }
    std::unique_ptr<PQconninfoOption, void(*)(PQconninfoOption*)> connOptions(
        PQconninfo(conn.get()),
        [](PQconninfoOption* opts){PQconninfoFree(opts);});
    if (! connOptions)
        Throw<std::runtime_error>("Can't get DB connection options.");

    std::size_t nfields = 0;
    for (PQconninfoOption* option = connOptions.get();
         option->keyword != nullptr; ++option)
    {
        if (++nfields > maxFields)
        {
            std::stringstream ss;
            ss << "DB returned connection options with > " << maxFields
               << " fields.";
            Throw<std::runtime_error>(ss.str());
        }

        if (! option->val
            || (remember_ip
                && (! strcmp(option->keyword, "hostaddr")
                    || ! strcmp(option->keyword, "port"))))
        {
            continue;
        }

        if (strlen(option->keyword) > maxFieldSize
            || strlen(option->val) > maxFieldSize)
        {
            std::stringstream ss;
            ss << "DB returned a connection option name or value with\n";
            ss << "excessive size (>" << maxFieldSize << " bytes).\n";
            ss << "option (possibly truncated): " <<
               std::string_view(option->keyword,
                   std::min(strlen(option->keyword), maxFieldSize))
               << '\n';
            ss << " value (possibly truncated): " <<
               std::string_view(option->val,
                   std::min(strlen(option->val), maxFieldSize));
            Throw<std::runtime_error>(ss.str());
        }
        config_.keywords.push_back(option->keyword);
        config_.values.push_back(option->val);
    }

    config_.keywordsIdx.reserve(config_.keywords.size() + 1);
    config_.valuesIdx.reserve(config_.values.size() + 1);
    for (std::size_t n = 0; n < config_.keywords.size(); ++n)
    {
        config_.keywordsIdx.push_back(config_.keywords[n].c_str());
        config_.valuesIdx.push_back(config_.values[n].c_str());
    }
    config_.keywordsIdx.push_back(nullptr);
    config_.valuesIdx.push_back(nullptr);

    get_if_exists(network_db_config, "max_connections",
        config_.max_connections);
    std::size_t timeout;
    if (get_if_exists(network_db_config, "timeout", timeout))
        config_.timeout = std::chrono::seconds(timeout);
}

void
PgPool::setup()
{
    {
        std::stringstream ss;
        ss << "max_connections: " << config_.max_connections << ", "
           << "timeout: " << config_.timeout.count() << ", "
           << "connection params: ";
        bool first = true;
        for (std::size_t i = 0; i < config_.keywords.size(); ++i)
        {
            if (first)
                first = false;
            else
                ss << ", ";
            ss << config_.keywords[i] << ": "
               << (config_.keywords[i] == "password" ? "*" :
                   config_.values[i]);
        }
        JLOG(j_.debug()) << ss.str();
    }
}

void
PgPool::stop()
{
    stop_ = true;
    std::lock_guard<std::mutex> lock(mutex_);
    idle_.clear();
}

void
PgPool::idleSweeper()
{
    std::size_t before, after;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        before = idle_.size();
        if (config_.timeout != std::chrono::seconds(0))
        {
            auto const found = idle_.upper_bound(
                clock_type::now() - config_.timeout);
            for (auto it = idle_.begin(); it != found; ++it)
            {
                idle_.erase(it);
                --connections_;
            }
        }
        after = idle_.size();
    }

    JLOG(j_.info()) << "Idle sweeper. connections: " << connections_
                    << ". checked out: " << connections_ - after
                    << ". idle before, after sweep: " << before << ", "
                    << after;
}

std::shared_ptr<Pg>
PgPool::checkout()
{
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (idle_.size())
        {
            auto entry = idle_.rbegin();
            auto ret = entry->second;
            idle_.erase(std::next(entry).base());
            return ret;
        }
        else if (connections_ < config_.max_connections && ! stop_)
        {
            ++connections_;
            return std::make_shared<Pg>(config_, j_);
        }
    }
    return {};
}

void
PgPool::checkin(std::shared_ptr<Pg>& pg)
{
    if (!pg)
        return;

    std::lock_guard<std::mutex> lock(mutex_);
    if (stop_ || ! *pg)
    {
        --connections_;
    }
    else
    {
        PGresult* res;
        while ((res = PQgetResult(pg->getConn())) != nullptr)
        {
            ExecStatusType const status = PQresultStatus(res);
            if (status == PGRES_COPY_IN)
            {
                if (PQputCopyEnd(pg->getConn(), nullptr) == -1)
                    pg.reset();
            }
            else if (status == PGRES_COPY_OUT || status == PGRES_COPY_BOTH)
            {
                pg.reset();
            }
        }

        if (pg)
            idle_.emplace(clock_type::now(), pg);
    }
    pg.reset();
}

//-----------------------------------------------------------------------------

pg_variant_type
PgQuery::queryVariant(pg_params const& dbParams, std::shared_ptr<Pg>& conn)
{
    while (!conn)
        conn = pool_->checkout();
    return conn->query(dbParams);
}

//-----------------------------------------------------------------------------

std::shared_ptr<PgPool>
make_PgPool(Section const& network_db_config, beast::Journal const j)
{
    if (network_db_config.empty())
    {
        return {};
    }
    else
    {
        auto ret = std::make_shared<PgPool>(network_db_config, j);
        ret->setup();
        return ret;
    }
}

void
initSchema(std::shared_ptr<PgPool> const& pool)
{
    static std::uint32_t const latest_schema_version = 1;

    static char const* version_query = R"(
CREATE TABLE IF NOT EXISTS version (version int NOT NULL);

DO $$
BEGIN
IF NOT EXISTS (SELECT 1 FROM version) THEN
INSERT INTO version VALUES (0);
END IF;
END $$;

SELECT version FROM version;
    )";

    auto res = PgQuery(pool).queryVariant({version_query, {}});
    if (std::holds_alternative<pg_error_type>(res))
    {
        std::stringstream ss;
        ss << "Error getting database schema version: "
            << std::get<pg_error_type>(res).second;
        Throw<std::runtime_error>(ss.str());
    }
    std::uint32_t const currentSchemaVersion =
        std::atoi(PQgetvalue(std::get<pg_result_type>(res).get(), 0, 0));
    std::cerr << "version: " << currentSchemaVersion << '\n';

    // Nothing to do if we are on the latest schema;
    if (currentSchemaVersion == latest_schema_version)
        return;

    switch (currentSchemaVersion)
    {
        case 0: // Install schema from scratch.
        {
            static char const* full_schema = R"(
CREATE TABLE IF NOT EXISTS ledgers (
    ledger_seq        bigint PRIMARY KEY,
    ledger_hash       bytea  NOT NULL,
    prev_hash         bytea  NOT NULL,
    total_coins       bigint NOT NULL,
    closing_time      bigint NOT NULL,
    prev_closing_time bigint NOT NULL,
    close_time_res    bigint NOT NULL,
    close_flags       bigint NOT NULL,
    account_set_hash  bytea  NOT NULL,
    trans_set_hash    bytea  NOT NULL
);

CREATE INDEX IF NOT EXISTS ledgers_ledger_hash_idx ON ledgers
    USING hash (ledger_hash);

CREATE TABLE IF NOT EXISTS transactions (
    ledger_seq bigint NOT NULL,
    transaction_index bigint NOT NULL,
    trans_id bytea NOT NULL,
    nodestore_hash bytea NOT NULL,
    constraint transactions_pkey PRIMARY KEY (ledger_seq, transaction_index),
    constraint transactions_fkey FOREIGN KEY (ledger_seq)
        REFERENCES ledgers (ledger_seq) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS transactions_trans_id_idx ON transactions
    USING hash (trans_id);

CREATE TABLE IF NOT EXISTS account_transactions (
    account           bytea  NOT NULL,
    ledger_seq        bigint NOT NULL,
    transaction_index bigint NOT NULL,
    constraint account_transactions_pkey PRIMARY KEY (account, ledger_seq,
        transaction_index),
    constraint account_transactions_fkey FOREIGN KEY (ledger_seq,
        transaction_index) REFERENCES transactions (
        ledger_seq, transaction_index) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS fki_account_transactions_idx ON
    account_transactions USING btree (ledger_seq, transaction_index);

CREATE OR REPLACE RULE ledgers_update_protect AS ON UPDATE TO
    ledgers DO INSTEAD NOTHING;

CREATE OR REPLACE RULE transactions_update_protect AS ON UPDATE TO
    transactions DO INSTEAD NOTHING;

CREATE OR REPLACE RULE account_transactions_update_protect AS ON UPDATE TO
    account_transactions DO INSTEAD NOTHING;

CREATE OR REPLACE FUNCTION set_version (
    _in_version int
) RETURNS void AS $$
BEGIN
    if _in_version IS NULL THEN RETURN; END IF;
    DELETE FROM version;
    EXECUTE 'INSERT INTO version VALUES ($1)' USING _in_version;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION tx (
    _in_trans_id bytea
) RETURNS jsonb AS $$
DECLARE
    _min_ledger        bigint := min_ledger();
    _min_seq           bigint := (SELECT ledger_seq
                                    FROM ledgers
                                   WHERE ledger_seq = _min_ledger
                                     FOR SHARE);
    _max_seq           bigint := max_ledger();
    _ledger_seq        bigint;
    _nodestore_hash    bytea;
BEGIN

    IF _min_seq IS NULL THEN
        RETURN jsonb_build_object('error', 'empty database');
    END IF;
    IF length(_in_trans_id) != 32 THEN
        RETURN jsonb_build_object('error', '_in_trans_id size: '
            || to_char(length(_in_trans_id), '999'));
    END IF;

    EXECUTE 'SELECT nodestore_hash, ledger_seq
               FROM transactions
              WHERE trans_id = $1
                AND ledger_seq BETWEEN $2 AND $3
    ' INTO _nodestore_hash, _ledger_seq USING _in_trans_id, _min_seq, _max_seq;
    IF _nodestore_hash IS NULL THEN
        RETURN jsonb_build_object('min_seq', _min_seq, 'max_seq', _max_seq);
    END IF;
    RETURN jsonb_build_object('nodestore_hash', _nodestore_hash, 'ledger_seq',
        _ledger_seq);
END;
$$ LANGUAGE plpgsql;

-- Return the earliest ledger sequence intended for range operations
-- that protect the bottom of the range from deletion. Return NULL if empty.
CREATE OR REPLACE FUNCTION min_ledger () RETURNS bigint AS $$
DECLARE
    _min_seq bigint := (SELECT ledger_seq from min_seq);
BEGIN
    IF _min_seq IS NULL THEN
        RETURN (SELECT ledger_seq FROM ledgers ORDER BY ledger_seq ASC LIMIT 1);
    ELSE
        RETURN _min_seq;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Return the latest ledger sequence in the database, or NULL if empty.
CREATE OR REPLACE FUNCTION max_ledger () RETURNS bigint AS $$
BEGIN
    RETURN (SELECT ledger_seq FROM ledgers ORDER BY ledger_seq DESC LIMIT 1);
END;
$$ LANGUAGE plpgsql;

/*
 * account_tx helper. From the rippled reporting process, only the
 * parameters without defaults are required. For the parameters with
 * defaults, validation should be done by rippled, such as:
 * _in_account_id should be a valid xrp base58 address.
 * _in_forward either true or false according to the published api
 * _in_limit should be validated and not simply passed through from
 *     client.
 *
 * For _in_ledger_index_min and _in_ledger_index_max, if passed in the
 * request, verify that their type is int and pass through as is.
 * For _ledger_hash, verify and convert from hex length 32 bytes and
 * prepend with \x (\\x C++).
 *
 * For _in_ledger_index, if the input type is integer, then pass through
 * as is. If the type is string and contents = validated, then do not
 * set _in_ledger_index. Instead set _in_invalidated to TRUE.
 *
 * There is no need for rippled to do any type of lookup on max/min
 * ledger range, lookup of hash, or the like. This functions does those
 * things, including error responses if bad input. Only the above must
 * be done to set the correct search range.
 *
 * If a marker is present in the request, verify the members 'ledger'
 * and 'seq' are integers and they correspond to _in_marker_seq
 * _in_marker_index.
 * To reiterate:
 * JSON input field 'ledger' corresponds to _in_marker_seq
 * JSON input field 'seq' corresponds to _in_marker_index
 */
CREATE OR REPLACE FUNCTION account_tx (
    _in_account_id bytea,
    _in_forward bool,
    _in_limit bigint,
    _in_ledger_index_min bigint = NULL,
    _in_ledger_index_max bigint = NULL,
    _in_ledger_hash      bytea  = NULL,
    _in_ledger_index     bigint = NULL,
    _in_validated bool   = NULL,
    _in_marker_seq       bigint = NULL,
    _in_marker_index     bigint = NULL
) RETURNS jsonb AS $$
DECLARE
    _min          bigint;
    _max          bigint;
    _sort_order   text       := (SELECT CASE WHEN _in_forward IS TRUE THEN
                                 'ASC' ELSE 'DESC' END);
    _marker       bool;
    _between_min  bigint;
    _between_max  bigint;
    _sql          text;
    _cursor       refcursor;
    _result       jsonb;
    _record       record;
    _tally        bigint     := 0;
    _ret_marker   jsonb;
    _transactions jsonb[]    := '{}';
BEGIN
    IF _in_ledger_index_min IS NOT NULL OR
            _in_ledger_index_max IS NOT NULL THEN
        _min := (SELECT CASE WHEN _in_ledger_index_min IS NULL
            THEN min_ledger() ELSE greatest(
            _in_ledger_index_min, min_ledger()) END);
        _max := (SELECT CASE WHEN _in_ledger_index_max IS NULL OR
            _in_ledger_index_max = -1 THEN max_ledger() ELSE
           least(_in_ledger_index_max, max_ledger()) END);

        IF _max < _min THEN
            RETURN jsonb_build_object('error', 'max is less than min ledger');
        END IF;

    ELSIF _in_ledger_hash IS NOT NULL OR _in_ledger_index IS NOT NULL
            OR _in_validated IS TRUE THEN
        IF _in_ledger_hash IS NOT NULL THEN
            IF length(_in_ledger_hash) != 32 THEN
                RETURN jsonb_build_object('error', '_in_ledger_hash size: '
                    || to_char(length(_in_ledger_hash), '999'));
            END IF;
            EXECUTE 'SELECT ledger_seq
                       FROM ledgers
                      WHERE ledger_hash = $1'
                INTO _min USING _in_ledger_hash::bytea;
        ELSE
            IF _in_ledger_index IS NOT NULL AND _in_validated IS TRUE THEN
                RETURN jsonb_build_object('error',
                    '_in_ledger_index cannot be set and _in_validated true');
            END IF;
            IF _in_validated IS TRUE THEN
                _in_ledger_index := max_ledger();
            END IF;
            _min := (SELECT ledger_seq
                       FROM ledgers
                      WHERE ledger_seq = _in_ledger_index);
        END IF;
        IF _min IS NULL THEN
            RETURN jsonb_build_object('error', 'ledger not found');
        END IF;
        _max := _min;
    ELSE
        _min := min_ledger();
        _max := max_ledger();
    END IF;

    IF _in_marker_seq IS NOT NULL OR _in_marker_index IS NOT NULL THEN
        _marker := TRUE;
        IF _in_marker_seq IS NULL OR _in_marker_index IS NULL THEN
            -- The rippled implementation returns no transaction results
            -- if either of these values are missing.
            _between_min := 0;
            _between_max := 0;
        ELSE
            IF _in_forward IS TRUE THEN
                _between_min := _in_marker_seq;
                _between_max := _max;
            ELSE
                _between_min := _min;
                _between_max := _in_marker_seq;
            END IF;
        END IF;
    ELSE
        _marker := FALSE;
        _between_min := _min;
        _between_max := _max;
    END IF;
    IF _between_max < _between_min THEN
        RETURN jsonb_build_object('error', 'ledger search range is '
            || to_char(_between_min, '999') || '-'
            || to_char(_between_max, '999'));
    END IF;

    _sql := format('
        SELECT transactions.ledger_seq, transactions.transaction_index,
               transactions.trans_id, transactions.nodestore_hash
          FROM transactions
               INNER JOIN account_transactions
                       ON transactions.ledger_seq =
                          account_transactions.ledger_seq
                          AND transactions.transaction_index =
                              account_transactions.transaction_index
         WHERE account_transactions.account = $1
           AND account_transactions.ledger_seq BETWEEN $2 AND $3
         ORDER BY transactions.ledger_seq %s, transactions.transaction_index %s
        ', _sort_order, _sort_order);

    OPEN _cursor FOR EXECUTE _sql USING _in_account_id, _between_min,
            _between_max;
    LOOP
        FETCH _cursor INTO _record;
        IF _record IS NULL THEN EXIT; END IF;
        IF _marker IS TRUE THEN
            IF _in_marker_seq = _record.ledger_seq THEN
                IF _in_forward IS TRUE THEN
                    IF _in_marker_index > _record.transaction_index THEN
                        CONTINUE;
                    END IF;
                ELSE
                    IF _in_marker_index < _record.transaction_index THEN
                        CONTINUE;
                    END IF;
                END IF;
            END IF;
            _marker := FALSE;
        END IF;

        _tally := _tally + 1;
        IF _tally > _in_limit THEN
            _ret_marker := jsonb_build_object(
                'ledger', _record.ledger_seq,
                'seq', _record.transaction_index);
            EXIT;
        END IF;

        -- Is the transaction index in the tx object?
        _transactions := _transactions || jsonb_build_object(
            'ledger_seq', _record.ledger_seq,
            'transaction_index', _record.transaction_index,
            'trans_id', _record.trans_id,
            'nodestore_hash', _record.nodestore_hash);

    END LOOP;
    CLOSE _cursor;

    _result := jsonb_build_object('ledger_index_min', _min,
        'ledger_index_max', _max,
        'transactions', _transactions);
    IF _ret_marker IS NOT NULL THEN
        _result := _result || jsonb_build_object('marker', _ret_marker);
    END IF;
    RETURN _result;
END;
$$ LANGUAGE plpgsql;

-- Trigger prior to insert on ledgers table. Validates length of hash fields.
-- Verifies ancestry based on ledger_hash & prev_hash as follows:
-- 1) If ledgers is empty, allows insert.
-- 2) For each new row, check for previous and later ledgers by a single
--    sequence. For each that exist, confirm ancestry based on hashes.
-- 3) Disallow inserts with no prior or next ledger by sequence if any
--    ledgers currently exist. This disallows gaps to be introduced by
--    way of inserting.
CREATE OR REPLACE FUNCTION insert_ancestry() RETURNS TRIGGER AS $$
DECLARE
    _parent bytea;
    _child  bytea;
BEGIN
    IF length(NEW.ledger_hash) != 32 OR length(NEW.prev_hash) != 32 THEN
        RAISE 'ledger_hash and prev_hash must each be 32 bytes: %', NEW;
    END IF;

    IF (SELECT ledger_hash
          FROM ledgers
         ORDER BY ledger_seq DESC
         LIMIT 1) = NEW.prev_hash THEN RETURN NEW; END IF;

    IF NOT EXISTS (SELECT 1 FROM LEDGERS) THEN RETURN NEW; END IF;

    _parent := (SELECT ledger_hash
                  FROM ledgers
                 WHERE ledger_seq = NEW.ledger_seq - 1);
    _child  := (SELECT prev_hash
                  FROM ledgers
                 WHERE ledger_seq = NEW.ledger_seq + 1);
    IF _parent IS NULL AND _child IS NULL THEN
        RAISE 'Ledger Ancestry error: orphan.';
    END IF;
    IF _parent != NEW.prev_hash THEN
        RAISE 'Ledger Ancestry error: bad parent.';
    END IF;
    IF _child != NEW.ledger_hash THEN
        RAISE 'Ledger Ancestry error: bad child.';
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION delete_ancestry () RETURNS TRIGGER AS $$
BEGIN
    IF EXISTS (SELECT 1
                 FROM ledgers
                WHERE ledger_seq = OLD.ledger_seq + 1)
            AND EXISTS (SELECT 1
                          FROM ledgers
                         WHERE ledger_seq = OLD.ledger_seq - 1) THEN
        RAISE 'Ledger Ancestry error: Can only delete the least or greatest '
              'ledger.';
    END IF;
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

-- Track the minimum sequence that should be used for ranged queries
-- with protection against deletion during the query. This should
-- be updated before calling online_delete() to not block deleting that
-- range.
CREATE TABLE IF NOT EXISTS min_seq (
    ledger_seq bigint NOT NULL
);

-- Set the minimum sequence for use in ranged queries with protection
-- against deletion greater than or equal to the input parameter. This
-- should be called prior to online_delete() with the same parameter
-- value so that online_delete() is not blocked by range queries
-- that are protected against concurrent deletion of the ledger at
-- the bottom of the range. This function needs to be called from a
-- separate transaction from that which executes online_delete().
CREATE OR REPLACE FUNCTION prepare_delete (
    _in_last_rotated bigint
) RETURNS void AS $$
BEGIN
    IF EXISTS (SELECT 1 FROM min_seq) THEN
        DELETE FROM min_seq;
    END IF;
    INSERT INTO min_seq VALUES (_in_last_rotated + 1);
END;
$$ LANGUAGE plpgsql;

-- Function to delete old data. All data belonging to ledgers prior to and
-- equal to the _in_seq parameter will be deleted. This should be
-- called with the input parameter equivalent to the value of lastRotated
-- in rippled's online_delete routine.
CREATE OR REPLACE FUNCTION online_delete (
    _in_seq bigint
) RETURNS void AS $$
BEGIN
    DELETE FROM LEDGERS WHERE ledger_seq <= _in_seq;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION delete_above (
    _in_seq bigint
) RETURNS void AS $$
DECLARE
    _max_seq bigint := max_ledger();
    _i bigint := _max_seq;
BEGIN
    IF _max_seq IS NULL THEN RETURN; END IF;
    LOOP
        IF _i <= _in_seq THEN RETURN; END IF;
        EXECUTE 'DELETE FROM ledgers WHERE ledger_seq = $1' USING _i;
        _i := _i - 1;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Verify correct ancestry of ledgers in database:
-- Table to persist last-confirmed latest ledger with proper ancestry.
CREATE TABLE IF NOT EXISTS ancestry_verified (
    ledger_seq bigint NOT NULL
);

-- Trigger to replace existing upon insert to ancestry_verified table.
-- Ensures only 1 row in that table.
CREATE OR REPLACE FUNCTION delete_verified() RETURNS TRIGGER AS $$
BEGIN
    IF EXISTS (SELECT 1 FROM ancestry_verified) THEN
        DELETE FROM ancestry_verified;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS ancestry_verified_trigger ON ancestry_verified;
CREATE TRIGGER ancestry_verified_trigger BEFORE INSERT ON ancestry_verified
    FOR EACH ROW EXECUTE PROCEDURE delete_verified();

-- Function to verify ancestry of ledgers based on ledger_hash and prev_hash.
-- Raises exception upon failure and prints current and parent rows.
-- _in_full: If TRUE, verify entire table. Else verify starting from
--           value in ancestry_verfied table. If no value, then start
--           from lowest ledger.
-- _in_persist: If TRUE, persist the latest ledger with correct ancestry.
--              If an exception was raised because of failure, persist
--              the latest ledger prior to that which failed.
-- _in_min: If set and _in_full is not true, the starting ledger from which
--          to verify.
-- _in_max: If set and _in_full is not true, the latest ledger to verify.
CREATE OR REPLACE FUNCTION check_ancestry (
    _in_full    bool = TRUE,
    _in_persist bool = TRUE,
    _in_min      bigint = NULL,
    _in_max      bigint = NULL
) RETURNS bigint AS $$
DECLARE
    _min                 bigint;
    _max                 bigint;
    _last_verified       bigint;
    _parent          ledgers;
    _current         ledgers;
    _cursor        refcursor;
BEGIN
    IF _in_full IS TRUE AND
            (_in_min IS NOT NULL) OR (_in_max IS NOT NULL) THEN
        RAISE 'Cannot specify manual range and do full check.';
    END IF;

    IF _in_min IS NOT NULL THEN
        _min := _in_min;
    ELSIF _in_full IS NOT TRUE THEN
        _last_verified := (SELECT ledger_seq FROM ancestry_verified);
        IF _last_verified IS NULL THEN
            _min := min_ledger();
        ELSE
            _min := _last_verified + 1;
        END IF;
    ELSE
        _min := min_ledger();
    END IF;
    EXECUTE 'SELECT * FROM ledgers WHERE ledger_seq = $1'
        INTO _parent USING _min - 1;
    IF _last_verified IS NOT NULL AND _parent IS NULL THEN
        RAISE 'Verified ledger % doesn''t exist.', _last_verified;
    END IF;

    IF _in_max IS NOT NULL THEN
        _max := _in_max;
    ELSE
        _max := max_ledger();
    END IF;

    OPEN _cursor FOR EXECUTE 'SELECT *
                                FROM ledgers
                               WHERE ledger_seq BETWEEN $1 AND $2
                               ORDER BY ledger_seq ASC'
                               USING _min, _max;
    LOOP
        FETCH _cursor INTO _current;
        IF _current IS NULL THEN EXIT; END IF;
        IF _parent IS NOT NULL THEN
            IF _current.prev_hash != _parent.ledger_hash THEN
                CLOSE _cursor;
                RETURN _current.ledger_seq;
                RAISE 'Ledger ancestry failure current, parent:% %',
                    _current, _parent;
            END IF;
        END IF;
        _parent := _current;
    END LOOP;
    CLOSE _cursor;

    IF _in_persist IS TRUE AND _parent IS NOT NULL THEN
        INSERT INTO ancestry_verified VALUES (_parent.ledger_seq);
    END IF;

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- Return number of whole seconds since the latest ledger was inserted, based
-- on ledger close time, not wall-clock of the insert.
-- Note that ledgers.closing_time is number of seconds since the XRP
-- epoch, which is 01/01/2000 00:00:00. This in turn is 946684800 seconds
-- after the UNIX epoch.
CREATE OR REPLACE FUNCTION age () RETURNS bigint AS $$
BEGIN
    RETURN (EXTRACT(EPOCH FROM (now())) -
        (946684800 + (SELECT closing_time
                        FROM ledgers
                       ORDER BY ledger_seq DESC
                       LIMIT 1)))::bigint;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION complete_ledgers () RETURNS text AS $$
DECLARE
    _min bigint := min_ledger();
    _max bigint := max_ledger();
BEGIN
    IF _min IS NULL THEN RETURN 'empty'; END IF;
    IF _min = _max THEN RETURN _min; END IF;
    RETURN _min || '-' || _max;
END;
$$ LANGUAGE plpgsql;

CREATE TABLE IF NOT EXISTS version (version int NOT NULL);

CREATE OR REPLACE FUNCTION schema_version (
    _in_version int = NULL
) RETURNS int AS $$
DECLARE
    _current_version int;
BEGIN
    IF _in_version IS NULL THEN
        RETURN (SELECT version FROM version LIMIT 1);
    END IF;
    IF EXISTS (SELECT 1 FROM version) THEN DELETE FROM version; END IF;
    INSERT INTO version VALUES (_in_version);
    RETURN _in_version;
END;
$$ LANGUAGE plpgsql;
            )";

            res = PgQuery(pool).queryVariant({full_schema, {}});
            if (std::holds_alternative<pg_error_type>(res))
            {
                std::stringstream ss;
                ss << "Error applying schema: "
                   << std::get<pg_error_type>(res).second;
                Throw<std::runtime_error>(ss.str());
            }

            auto cmd = boost::format(R"(SELECT set_version(%u))");
            res = PgQuery(pool).queryVariant(
                {boost::str(cmd % latest_schema_version).c_str(), {}});
            if (std::holds_alternative<pg_error_type>(res))
            {
                std::stringstream ss;
                ss << "Error setting schema version to "
                   << latest_schema_version << ": "
                   << std::get<pg_error_type>(res).second;
                Throw<std::runtime_error>(ss.str());
            }


            break;
        }
            /*
        case 1:
            ;
        case 2:
            ;
        case 3:
            ;
        case 4:
            ;
            break;
             */
        default:
        {
            std::stringstream ss;
            ss << "Postgres server schema version " << currentSchemaVersion
                << " has no case to determine whether to upgrade.";
            assert(false);
            Throw<std::runtime_error>(ss.str());
        }

    }
}

} // ripple
