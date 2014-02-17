from datetime import datetime
import json
import string

from aludel.database import TableCollection, make_table, CollectionMissingError
from sqlalchemy import Column, Integer, String, DateTime, Boolean, Text
from sqlalchemy.sql import select, func
from twisted.internet.defer import inlineCallbacks, returnValue


class UniqueCodeError(Exception):
    pass


class AuditMismatch(UniqueCodeError):
    pass


class NoUniqueCodePool(UniqueCodeError):
    pass


class CannotRedeemUniqueCode(UniqueCodeError):
    def __init__(self, reason, unique_code):
        super(CannotRedeemUniqueCode, self).__init__(reason)
        self.reason = reason
        self.unique_code = unique_code


class UniqueCodePool(TableCollection):
    # We assume all unique codes match this.
    UNIQUE_CODE_ALLOWED_CHARS = string.lowercase + string.digits

    unique_codes = make_table(
        Column("id", Integer(), primary_key=True),
        Column("unique_code", String(255), nullable=False, index=True),
        Column("flavour", String(255), index=True),
        Column("used", Boolean(), default=False, index=True),
        Column("created_at", DateTime(timezone=False)),
        Column("modified_at", DateTime(timezone=False)),
        Column("reason", String(255), default=None),
    )

    audit = make_table(
        Column("id", Integer(), primary_key=True),
        Column("request_id", String(255), nullable=False, index=True,
               unique=True),
        Column("transaction_id", String(255), nullable=False, index=True),
        Column("user_id", String(255), nullable=False, index=True),
        Column("request_data", Text(), nullable=False),
        Column("response_data", Text(), nullable=False),
        Column("error", Boolean(), nullable=False),
        Column("created_at", DateTime(timezone=False)),
        Column("unique_code", String(255), index=True),
    )

    import_audit = make_table(
        Column("id", Integer(), primary_key=True),
        Column("request_id", String(255), nullable=False, index=True,
               unique=True),
        Column("content_md5", String(255), nullable=False),
        Column("created_at", DateTime(timezone=False)),
    )

    @inlineCallbacks
    def execute_query(self, query, *args, **kw):
        try:
            result = yield super(UniqueCodePool, self).execute_query(
                query, *args, **kw)
        except CollectionMissingError:
            raise NoUniqueCodePool(self.name)
        returnValue(result)

    @classmethod
    def canonicalise_unique_code(cls, unique_code):
        """Clean spurious characters out of unique code entries.

        The unique_code is cleaned by converting it to lowercase and stripping
        out any characters not in :attr:`UNIQUE_CODE_ALLOWED_CHARS`.
        """
        return ''.join(c for c in unique_code.lower()
                       if c in cls.UNIQUE_CODE_ALLOWED_CHARS)

    def _audit_request(self, audit_params, req_data, resp_data, unique_code,
                       error=False):
        request_id = audit_params['request_id']
        transaction_id = audit_params['transaction_id']
        user_id = audit_params['user_id']
        return self.execute_query(
            self.audit.insert().values(
                request_id=request_id,
                transaction_id=transaction_id,
                user_id=user_id,
                request_data=json.dumps(req_data),
                response_data=json.dumps(resp_data),
                error=error,
                created_at=datetime.utcnow(),
                unique_code=unique_code,
            ))

    @inlineCallbacks
    def _get_previous_request(self, audit_params, req_data):
        rows = yield self.execute_fetchall(
            self.audit.select().where(
                self.audit.c.request_id == audit_params['request_id']))
        if not rows:
            returnValue(None)
        [row] = rows
        old_audit_params = {
            'request_id': row['request_id'],
            'transaction_id': row['transaction_id'],
            'user_id': row['user_id'],
        }
        old_req_data = json.loads(row['request_data'])
        old_resp_data = json.loads(row['response_data'])
        if audit_params != old_audit_params or req_data != old_req_data:
            raise AuditMismatch()

        if row['error']:
            raise CannotRedeemUniqueCode(
                old_resp_data['reason'], old_resp_data['unique_code'])
        returnValue(json.loads(row['response_data']))

    @inlineCallbacks
    def import_unique_codes(self, request_id, content_md5, unique_code_dicts):
        trx = yield self._conn.begin()

        # Check if we've already done this one.
        rows = yield self.execute_fetchall(
            self.import_audit.select().where(
                self.import_audit.c.request_id == request_id))
        if rows:
            yield trx.rollback()
            [row] = rows
            if row['content_md5'] == content_md5:
                returnValue(None)
            else:
                raise AuditMismatch(row['content_md5'])

        yield self.execute_query(
            self.import_audit.insert().values(
                request_id=request_id,
                content_md5=content_md5,
                created_at=datetime.utcnow(),
            ))

        # NOTE: We're assuming that this will be fast enough. If it isn't,
        # we'll need to make a database-specific plan of some kind.
        now = datetime.utcnow()
        unique_code_rows = [{
            'flavour': unique_code_dict['flavour'],
            'unique_code': unique_code_dict['unique_code'],
            'created_at': now,
            'modified_at': now,
        } for unique_code_dict in unique_code_dicts]
        result = yield self.execute_query(
            self.unique_codes.insert(), unique_code_rows)
        yield trx.commit()
        returnValue(result)

    def _format_unique_code(self, unique_code_row, fields=None):
        if fields is None:
            fields = set(f for f in unique_code_row.keys()
                         if f not in ('created_at', 'modified_at'))
        return dict((k, v) for k, v in unique_code_row.items() if k in fields)

    @inlineCallbacks
    def _get_unique_code(self, canonical_code):
        result = yield self.execute_query(
            self.unique_codes.select().where(
                self.unique_codes.c.unique_code == canonical_code,
            ).limit(1))
        unique_code = yield result.fetchone()
        if unique_code is not None:
            unique_code = self._format_unique_code(unique_code)
        returnValue(unique_code)

    def _update_unique_code(self, unique_code_id, **values):
        values['modified_at'] = datetime.utcnow()
        return self.execute_query(
            self.unique_codes.update().where(
                self.unique_codes.c.id == unique_code_id).values(**values))

    @inlineCallbacks
    def _redeem_unique_code(self, canonical_code, reason):
        unique_code = yield self._get_unique_code(canonical_code)
        if unique_code is None:
            raise CannotRedeemUniqueCode('invalid', canonical_code)
        if unique_code['used']:
            raise CannotRedeemUniqueCode('used', canonical_code)
        yield self._update_unique_code(
            unique_code['id'], used=True, reason=reason)
        returnValue(unique_code)

    @inlineCallbacks
    def redeem_unique_code(self, candidate_code, audit_params):
        audit_req_data = {'candidate_code': candidate_code}

        # If we have already seen this request, return the same response as
        # before. Appropriate exceptions will be raised here.
        previous_data = yield self._get_previous_request(
            audit_params, audit_req_data)
        if previous_data is not None:
            returnValue(previous_data)

        # We do this after the audit check so we don't loosen the conditions.
        candidate_code = self.canonicalise_unique_code(candidate_code)

        # This is a new request, so handle it accordingly.
        trx = yield self._conn.begin()
        try:
            unique_code = yield self._redeem_unique_code(
                candidate_code, 'redeemed')
        except CannotRedeemUniqueCode as e:
            audit_resp_data = {
                'reason': e.reason,
                'unique_code': e.unique_code,
            }
            yield self._audit_request(
                audit_params, audit_req_data, audit_resp_data, candidate_code,
                error=True)
            raise e
        else:
            yield self._audit_request(
                audit_params, audit_req_data, unique_code, candidate_code)
        finally:
            yield trx.commit()
        returnValue(unique_code)

    def count_unique_codes(self):
        return self.execute_fetchall(
            select([
                self.unique_codes.c.flavour,
                self.unique_codes.c.used,
                func.count().label('count'),
            ]).group_by(
                self.unique_codes.c.flavour,
                self.unique_codes.c.used,
            )
        )

    @inlineCallbacks
    def _query_audit(self, where_clause):
        rows = yield self.execute_fetchall(
            self.audit.select().where(where_clause).order_by(
                self.audit.c.created_at))
        returnValue([{
            'request_id': row['request_id'],
            'transaction_id': row['transaction_id'],
            'user_id': row['user_id'],
            'request_data': json.loads(row['request_data']),
            'response_data': json.loads(row['response_data']),
            'error': row['error'],
            'created_at': row['created_at'],
        } for row in rows])

    def query_by_request_id(self, request_id):
        return self._query_audit(self.audit.c.request_id == request_id)

    def query_by_transaction_id(self, transaction_id):
        return self._query_audit(self.audit.c.transaction_id == transaction_id)

    def query_by_user_id(self, user_id):
        return self._query_audit(self.audit.c.user_id == user_id)

    def query_by_unique_code(self, unique_code):
        return self._query_audit(self.audit.c.unique_code == unique_code)
