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

#include <ripple/app/reporting/DBHelpers.h>
#include <memory>

namespace ripple {

bool
writeToLedgersDB(
    LedgerInfo const& info,
    std::shared_ptr<PgQuery>& pgQuery,
    std::shared_ptr<Pg>& conn,
    beast::Journal& j)
{
    JLOG(j.debug()) << __func__;
    auto cmd = boost::format(
        R"(INSERT INTO ledgers
                VALUES(%u,'\x%s', '\x%s',%u,%u,%u,%u,%u,'\x%s','\x%s')
                )");

    auto ledgerInsert = boost::str(
        cmd % info.seq % strHex(info.hash) % strHex(info.parentHash) %
        info.drops.drops() % info.closeTime.time_since_epoch().count() %
        info.parentCloseTime.time_since_epoch().count() %
        info.closeTimeResolution.count() % info.closeFlags %
        strHex(info.accountHash) % strHex(info.txHash));
    JLOG(j.trace()) << __func__ << " : "
                    << " : "
                    << "query string = " << ledgerInsert;

    auto res = pgQuery->queryVariant({ledgerInsert.data(), {}}, conn);

    return !std::holds_alternative<pg_error_type>(res);
}

void
bulkWriteToTable(
    std::shared_ptr<PgQuery>& pgQuery,
    std::shared_ptr<Pg>& conn,
    char const* copyQuery,
    std::string const bufString,
    beast::Journal& j)
{
    JLOG(j.debug()) << __func__;
    //    while (!etl.isStopping())
        // Initiate COPY operation

    auto res = pgQuery->queryVariant({copyQuery, {}}, conn);
    if (!std::holds_alternative<pg_result_type>(res) ||
        PQresultStatus(std::get<pg_result_type>(res).get()) != PGRES_COPY_IN)
    {
        std::stringstream msg;
        msg << "bulkWriteToTable : Postgres insert error: ";
        msg << PQerrorMessage(conn->getConn());
        Throw<std::runtime_error>(msg.str());
    }

    auto rawRes =
        PQputCopyData(conn->getConn(), bufString.c_str(), bufString.size());
    if(rawRes == 0 || rawRes == -1)
    {
        std::stringstream msg;
        msg << "bulkWriteToTable : Postgres insert error: ";
        msg << PQerrorMessage(conn->getConn());
        msg << " : rawRes = " << rawRes;
        Throw<std::runtime_error>(msg.str());
    }
    auto pqResult = PQgetResult(conn->getConn());
    auto pqResultStatus = PQresultStatus(pqResult);
   
    if(pqResultStatus != PGRES_COPY_IN)
    {
        std::stringstream msg;
        msg << "bulkWriteToTable : Postgres insert error: ";
        msg << PQerrorMessage(conn->getConn());
        msg << " : result status = ";
        msg << pqResultStatus;
        Throw<std::runtime_error>(msg.str());
    }

    PQclear(pqResult);

    rawRes = PQputCopyEnd(conn->getConn(), nullptr);
    if(rawRes == 0 || rawRes == -1)
    {
        std::stringstream msg;
        msg << "bulkWriteToTable : Postgres insert error: ";
        msg << PQerrorMessage(conn->getConn());
        msg << " : rawRes = " << rawRes;
        Throw<std::runtime_error>(msg.str());
    }
    pqResult = PQgetResult(conn->getConn());
    pqResultStatus = PQresultStatus(pqResult);
    if(pqResultStatus != PGRES_COMMAND_OK)
    {
        std::stringstream msg;
        msg << "bulkWriteToTable : Postgres insert error: ";
        msg << PQerrorMessage(conn->getConn());
        msg << " : result status = " << pqResultStatus;
        Throw<std::runtime_error>(msg.str());
    }
    PQclear(pqResult);
    //        while (!etl.isStopping())
    while (true)
    {
        pqResult = PQgetResult(conn->getConn());
        if (!pqResult)
            break;
        pqResultStatus = PQresultStatus(pqResult);
    }
    if(pqResultStatus != PGRES_COMMAND_OK)
    {
        std::stringstream msg;
        msg << "bulkWriteToTable : Postgres insert error: ";
        msg << PQerrorMessage(conn->getConn());
        msg << " : result status = " << pqResultStatus;
        Throw<std::runtime_error>(msg.str());
    }
}

bool
writeToPostgres(
    LedgerInfo const& info,
    std::vector<AccountTransactionsData>& accountTxData,
    std::shared_ptr<PgPool> const& pgPool,
    bool useTxTables,
    beast::Journal& j)
{
    // TODO: clean this up a bit. use less auto, better error handling, etc
    JLOG(j.debug()) << __func__ << " : "
                    << "Beginning write to Postgres";
    if (!pgPool)
    {
        JLOG(j.fatal()) << __func__ << " : "
                        << "app_.pgPool is null";
        Throw<std::runtime_error>("pgPool is null");
    }
    std::shared_ptr<PgQuery> pg = std::make_shared<PgQuery>(pgPool);
    std::shared_ptr<Pg> conn;

    auto res = pg->queryVariant({"BEGIN", {}}, conn);
    if (!std::holds_alternative<pg_result_type>(res) ||
        PQresultStatus(std::get<pg_result_type>(res).get()) != PGRES_COMMAND_OK)
    {
        std::stringstream msg;
        msg << "bulkWriteToTable : Postgres insert error: ";
        msg << PQerrorMessage(conn->getConn());
        Throw<std::runtime_error>(msg.str());
    }

    // Writing to the ledgers db fails if the ledger already exists in the db.
    // In this situation, the ETL process has detected there is another writer,
    // and falls back to only publishing
    if (!writeToLedgersDB(info, pg, conn, j))
    {
        pgPool->checkin(conn);

        JLOG(j.warn()) << __func__ << " : "
                       << "Failed to write to ledgers database.";
        return false;
    }

    if (useTxTables)
    {
        std::stringstream transactionsCopyBuffer;
        std::stringstream accountTransactionsCopyBuffer;
        for (auto& data : accountTxData)
        {
            std::string txHash = strHex(data.txHash);
            std::string nodestoreHash = strHex(data.nodestoreHash);
            auto idx = data.transactionIndex;
            auto ledgerSeq = data.ledgerSequence;

            transactionsCopyBuffer << std::to_string(ledgerSeq) << '\t'
                                   << std::to_string(idx) << '\t' << "\\\\x"
                                   << txHash 
                                   <<'\t' << "\\\\x" << nodestoreHash
                                   << '\n';

            for (auto& a : data.accounts)
            {
                std::string acct = strHex(a);
                accountTransactionsCopyBuffer
                    << "\\\\x" << acct << '\t' << std::to_string(ledgerSeq)
                    << '\t' << std::to_string(idx) << '\n';
            }
        }
        JLOG(j.debug()) << "transactions: " << transactionsCopyBuffer.str();
        JLOG(j.debug()) << "account_transactions: "
                        << accountTransactionsCopyBuffer.str();

        bulkWriteToTable(
            pg,
            conn,
            "COPY transactions FROM stdin",
            transactionsCopyBuffer.str(),
            j);
        bulkWriteToTable(
            pg,
            conn,
            "COPY account_transactions FROM stdin",
            accountTransactionsCopyBuffer.str(),
            j);
    }

    res = pg->queryVariant({"COMMIT", {}}, conn);
    if(!std::holds_alternative<pg_result_type>(res) ||
        PQresultStatus(std::get<pg_result_type>(res).get()) !=
        PGRES_COMMAND_OK)
    {
        std::stringstream msg;
        msg << "bulkWriteToTable : Postgres insert error: ";
        msg << PQerrorMessage(conn->getConn());
        Throw<std::runtime_error>(msg.str());
    }

    pgPool->checkin(conn);

    JLOG(j.info()) << __func__ << " : "
                   << "Successfully wrote to Postgres";
    return true;
}

}  // namespace ripple
