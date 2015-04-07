//------------------------------------------------------------------------------
/*
    This file is part of rippled: https://github.com/ripple/rippled
    Copyright (c) 2012, 2013 Ripple Labs Inc.

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

#include <ripple/lib/ripple.h>
#include <ripple/protocol/RippleAddress.h>
#include <ripple/crypto/impl/ec_key.h>
#include <ripple/crypto/ECDSA.h>
#include <ripple/protocol/HashPrefix.h>
#include <ripple/json/json_reader.h>
#include <ripple/json/json_writer.h>
#include <ripple/rpc/impl/TransactionSign.h>
#include <beast/cxx14/memory.h>
#include <openssl/ec.h>
#include <openssl/sha.h>
#include <iostream>
#include <cstdint>
#include <iomanip>

void ripple_transaction_sign (void const* inbuf, size_t const len,
        char** outbuf, size_t* outbuflen)
{
    Json::Value jvRequest;
    Json::Reader jrReader;
    if (jrReader.parse (static_cast <char const*>(inbuf),
            static_cast <char const*>(inbuf) + len, jvRequest))
    {
        Json::Value::Members mem = jvRequest.getMemberNames();
        std::string tx_blob = ripple::RPC::transactionSign (jvRequest);

        *outbuflen = tx_blob.size();
        if (*outbuflen)
        {
            *outbuf = new char[*outbuflen];
            std::copy (tx_blob.begin(), tx_blob.end(), *outbuf);
        }
        else
        {
            std::cerr << "no results from transactionSign()\n";
            *outbuf = nullptr;
        }
    }
    else
    {
        std::cerr << "bad jrReader.parse()\n";
        *outbuf = nullptr;
    }
}

void ripple_free (char* outbuf)
{
    delete[] outbuf;
}
