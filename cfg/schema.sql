\set ON_ERROR_STOP on
SET client_min_messages = WARNING;
SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: plpgsql; Type: EXTENSION; Schema: -; Owner: -
--

-- CREATE EXTENSION IF NOT EXISTS plpgsql WITH SCHEMA pg_catalog;

--
-- Name: EXTENSION plpgsql; Type: COMMENT; Schema: -; Owner: -
--

-- COMMENT ON EXTENSION plpgsql IS 'PL/pgSQL procedural language';

SET default_tablespace = '';

SET default_with_oids = false;

--
-- Name: account_transactions; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE IF NOT EXISTS public.transactions (
    ledger_seq bigint NOT NULL,
    transaction_index bigint NOT NULL,
    trans_id bytea NOT NULL,
    nodestore_hash bytea NOT NULL,
    constraint transactions_pkey PRIMARY KEY (ledger_seq, transaction_index),
    constraint transactions_fkey FOREIGN KEY (ledger_seq)
        REFERENCES public.transactions (ledger_seq) ON DELETE CASCADE
);

--ALTER TABLE public.transactions ADD CONSTRAINT transactions_pkey PRIMARY KEY (
--    ledger_seq, transaction_index);

CREATE INDEX IF NOT EXISTS transactions_trans_id_idx ON public.transactions
    USING hash (trans_id);

CREATE TABLE IF NOT EXISTS public.account_transactions (
    account           bytea  NOT NULL,
    ledger_seq        bigint NOT NULL,
    transaction_index bigint NOT NULL,
    constraint account_transactions_pkey PRIMARY KEY (account, ledger_seq,
        transaction_index),
    constraint account_transactions_fkey FOREIGN KEY (ledger_seq,
        transaction_index) REFERENCES public.account_transactions (
        ledger_seq, transaction_index) ON DELETE CASCADE
);

--
-- Name: ledgers; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE IF NOT EXISTS public.ledgers (
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

CREATE INDEX IF NOT EXISTS ledgers_ledger_hash_idx ON public.ledgers
    USING hash (ledger_hash);

--
-- Name: account_transactions account_transactions_pkey;
-- Type: CONSTRAINT; Schema: public; Owner: -
--

--ALTER TABLE ONLY public.account_transactions
--    ADD CONSTRAINT account_transactions_pkey PRIMARY KEY (
--    account, ledger_seq, transaction_index);

--
-- Name: ledgers ledgers_hash_unique; Type: CONSTRAINT;
-- Schema: public; Owner: -
--

--ALTER TABLE ONLY public.ledgers
--    ADD CONSTRAINT ledgers_hash_unique UNIQUE (ledger_hash);

--ALTER TABLE ONLY public.ledgers
--    ADD CONSTRAINT ledgers_parent_hash_unique UNIQUE (parent_hash);

--
-- Name: ledgers ledgers_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

--ALTER TABLE ONLY public.ledgers
--    ADD CONSTRAINT ledgers_pkey PRIMARY KEY (ledger_seq);

CREATE OR REPLACE RULE ledgers_update_protect AS ON UPDATE TO
    public.ledgers DO INSTEAD NOTHING;

CREATE OR REPLACE RULE transactions_update_protect AS ON UPDATE TO
    public.transactions DO INSTEAD NOTHING;

CREATE OR REPLACE RULE account_transactions_update_protect AS ON UPDATE TO
    public.account_transactions DO INSTEAD NOTHING;

--
-- Name: transactions transactions_pkey; Type: CONSTRAINT;
-- Schema: public; Owner: -
--

--ALTER TABLE ONLY public.transactions
--    ADD CONSTRAINT transactions_pkey PRIMARY KEY (
--    ledger_seq, transaction_index);

--CREATE RULE transactions_update_protect AS ON UPDATE TO
--    public.transactions DO INSTEAD NOTHING;

--
-- Name: fki_account_transactions_fkey; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX IF NOT EXISTS fki_account_transactions_idx ON
    public.account_transactions USING btree (ledger_seq, transaction_index);


--
-- Name: transactions_hash_idx; Type: INDEX; Schema: public; Owner: -
--

--CREATE INDEX transactions_hash_idx ON public.transactions
--    USING btree (((tx ->> 'hash'::text)));


--
-- Name: account_transactions account_transactions_fkey;
-- Type: FK CONSTRAINT; Schema: public; Owner: -
--

--ALTER TABLE ONLY public.account_transactions
--    ADD CONSTRAINT account_transactions_fkey FOREIGN KEY (
--    ledger_seq, transaction_index) REFERENCES public.transactions(
--    ledger_seq, transaction_index) ON DELETE CASCADE;

--CREATE RULE account_transactions_update_protect AS ON UPDATE TO
--    public.account_transactions DO INSTEAD NOTHING;

--
-- Name: account_transactions account_transactions_fkey; Type: FK CONSTRAINT;
-- Schema: public; Owner: -
--

--ALTER TABLE ONLY public.transactions
--    ADD CONSTRAINT transactions_fkey FOREIGN KEY (ledger_seq)
--    REFERENCES public.ledgers(ledger_seq)
--    ON DELETE CASCADE;

--ALTER TABLE ONLY public.account_transactions
--    ADD CONSTRAINT account_transactions_fkey FOREIGN KEY (
--        ledger_seq, transaction_index)
--    REFERENCES public.transactions(ledger_seq, transaction_index)
--    ON DELETE CASCADE;

