-- Deploying this file idempotentently configures custom functions and
-- related objects.

\set ON_ERROR_STOP on
SET client_min_messages = WARNING;

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
    RETURN jsonb_build_object('nodestore_hash', _nodestore_hash, 'ledger_seq', _ledger_seq);
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

--DROP TYPE IF EXISTS mode_enum;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'mode_enum') THEN
        CREATE TYPE mode_enum AS ENUM ('reporting', 'tx');
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS mode (mode mode_enum NOT NULL);

CREATE OR REPLACE FUNCTION set_mode (
    _in_mode mode_enum
) RETURNS bigint AS $$
DECLARE
    _current_mode mode_enum := (SELECT mode FROM mode LIMIT 1);
    _ancestry bigint;
BEGIN
    IF _in_mode = _current_mode OR _in_mode IS NULL THEN RETURN NULL; END IF;

    IF _in_mode = 'reporting' AND _current_mode = 'tx' THEN
        _ancestry := check_ancestry();
        IF _ancestry IS NOT NULL THEN RETURN _ancestry; END IF;
    END IF;

    DROP TRIGGER IF EXISTS ancestry_insert_trigger ON ledgers;
    DROP TRIGGER IF EXISTS ancestry_delete_trigger ON ledgers;

    IF _in_mode = 'reporting' THEN
        CREATE TRIGGER ancestry_insert_trigger BEFORE INSERT ON ledgers
            FOR EACH ROW EXECUTE PROCEDURE insert_ancestry();
        CREATE TRIGGER ancestry_delete_trigger BEFORE DELETE ON ledgers
            FOR EACH ROW EXECUTE PROCEDURE delete_ancestry();
    END IF;

    DELETE FROM mode;
    INSERT INTO mode VALUES (_in_mode);

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION schema_is_current (
    _in_version int
)
RETURNS bool AS $$
DECLARE
    _current_version int := (SELECT version FROM version LIMIT 1);
BEGIN
    IF _current_version IS NULL OR _in_version IS NULL THEN
        RETURN FALSE;
    END IF;
    RETURN _in_version = _current_version;
END;
$$ LANGUAGE plpgsql;

