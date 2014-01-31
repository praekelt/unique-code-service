from datetime import datetime, timedelta
import os

from aludel.database import get_engine, MetaData
from aludel.tests.doubles import FakeReactorThreads
from twisted.trial.unittest import TestCase

from unique_code_service.models import (
    UniqueCodePool, CannotRedeemUniqueCode, NoUniqueCodePool, AuditMismatch,
)

from .helpers import populate_pool, mk_audit_params


def round_dt(dt):
    """
    Return a copy of ``dt`` rounded to the nearest second.

    MySQL apparently does this for its DATETIME fields.
    """
    new_dt = dt.replace(microsecond=0)
    if dt.microsecond >= 500000:
        new_dt += timedelta(seconds=1)
    return new_dt


class TestUniqueCodePool(TestCase):
    timeout = 5

    def setUp(self):
        connection_string = os.environ.get(
            "ALUDEL_TEST_CONNECTION_STRING", "sqlite://")
        self.engine = get_engine(
            connection_string, reactor=FakeReactorThreads())
        self._drop_tables()
        self.conn = self.successResultOf(self.engine.connect())

    def tearDown(self):
        self.successResultOf(self.conn.close())
        self._drop_tables()
        assert self.successResultOf(self.engine.table_names()) == []

    def _drop_tables(self):
        # NOTE: This is a blocking operation!
        md = MetaData(bind=self.engine._engine)
        md.reflect()
        md.drop_all()

    def assert_unique_code_counts(self, pool, expected_rows):
        rows = self.successResultOf(pool.count_unique_codes())
        assert sorted(tuple(r) for r in rows) == sorted(expected_rows)

    def test_import_fails_for_missing_pool(self):
        pool = UniqueCodePool('testpool', self.conn)
        f = self.failureResultOf(pool.count_unique_codes(), NoUniqueCodePool)
        assert f.value.args == ('testpool',)
        f = self.failureResultOf(
            populate_pool(pool, ['vanilla'], [0]), NoUniqueCodePool)
        assert f.value.args == ('testpool',)

    def test_exists(self):
        pool = UniqueCodePool('testpool', self.conn)
        assert not self.successResultOf(pool.exists())
        self.successResultOf(pool.create_tables())
        assert self.successResultOf(pool.exists())

    def test_import_unique_codes(self):
        pool = UniqueCodePool('testpool', self.conn)
        self.successResultOf(pool.create_tables())

        self.successResultOf(pool.import_unique_codes('req-0', 'md5-0', [
            {'flavour': 'vanilla', 'unique_code': 'v0'},
            {'flavour': 'vanilla', 'unique_code': 'v1'},
            {'flavour': 'chocolate', 'unique_code': 'c0'},
            {'flavour': 'chocolate', 'unique_code': 'c1'},
        ]))
        self.assert_unique_code_counts(pool, [
            ('vanilla', False, 2),
            ('chocolate', False, 2),
        ])

    def test_import_unique_codes_idempotence(self):
        pool = UniqueCodePool('testpool', self.conn)
        self.successResultOf(pool.create_tables())

        unique_codes = [
            {'flavour': 'vanilla', 'unique_code': 'v0'},
            {'flavour': 'vanilla', 'unique_code': 'v1'},
            {'flavour': 'chocolate', 'unique_code': 'c0'},
            {'flavour': 'chocolate', 'unique_code': 'c1'},
        ]

        expected_codes = [
            ('vanilla', False, 2),
            ('chocolate', False, 2),
        ]

        self.successResultOf(
            pool.import_unique_codes('req-0', 'md5-0', unique_codes))
        self.assert_unique_code_counts(pool, expected_codes)

        # Again, with the same request.
        self.successResultOf(
            pool.import_unique_codes('req-0', 'md5-0', unique_codes))
        self.assert_unique_code_counts(pool, expected_codes)

        # Same request, different content.
        self.failureResultOf(
            pool.import_unique_codes('req-0', 'md5-1', unique_codes[:1]),
            AuditMismatch)
        self.assert_unique_code_counts(pool, expected_codes)

    def test_canonicalise_unique_code(self):
        assert 'foo' == UniqueCodePool.canonicalise_unique_code('foo')
        assert 'foo' == UniqueCodePool.canonicalise_unique_code('FOO')
        assert 'foo' == UniqueCodePool.canonicalise_unique_code('fOo')
        assert 'foo' == UniqueCodePool.canonicalise_unique_code('f O:-o')
        assert 'foo' != UniqueCodePool.canonicalise_unique_code('f00')
        alnum_lower = '0123456789' + 'abcdefghijklmnopqrstuvwxyz' * 2
        assert alnum_lower == UniqueCodePool.canonicalise_unique_code(
            ''.join(chr(i) for i in range(128)))

    def test_redeem_valid_unique_code(self):
        pool = UniqueCodePool('testpool', self.conn)
        self.successResultOf(pool.create_tables())
        populate_pool(pool, ['vanilla'], [0])
        self.assert_unique_code_counts(pool, [('vanilla', False, 1)])

        unique_code = self.successResultOf(
            pool.redeem_unique_code('vanilla0', mk_audit_params('req-0')))
        assert unique_code['flavour'] == 'vanilla'
        assert unique_code['unique_code'] == 'vanilla0'
        self.assert_unique_code_counts(pool, [('vanilla', True, 1)])

    def test_redeem_invalid_unique_code(self):
        pool = UniqueCodePool('testpool', self.conn)
        self.successResultOf(pool.create_tables())
        self.assert_unique_code_counts(pool, [])

        failure = self.failureResultOf(
            pool.redeem_unique_code('vanilla0', mk_audit_params('req-0')),
            CannotRedeemUniqueCode)
        assert failure.value.reason == 'invalid'
        assert failure.value.unique_code == 'vanilla0'

    def test_redeem_used_unique_code(self):
        pool = UniqueCodePool('testpool', self.conn)
        self.successResultOf(pool.create_tables())
        populate_pool(pool, ['vanilla'], [0])
        self.successResultOf(
            pool.redeem_unique_code('vanilla0', mk_audit_params('req-0')))
        self.assert_unique_code_counts(pool, [('vanilla', True, 1)])

        failure = self.failureResultOf(
            pool.redeem_unique_code('vanilla0', mk_audit_params('req-1')),
            CannotRedeemUniqueCode)
        assert failure.value.reason == 'used'
        assert failure.value.unique_code == 'vanilla0'

    def test_redeem_unique_code_idempotent(self):
        pool = UniqueCodePool('testpool', self.conn)
        self.successResultOf(pool.create_tables())
        populate_pool(pool, ['vanilla'], [0])
        self.assert_unique_code_counts(pool, [('vanilla', False, 1)])

        audit_params = mk_audit_params('req-0')

        # Redeem a code successfully.
        unique_code = self.successResultOf(
            pool.redeem_unique_code('vanilla0', audit_params))
        assert unique_code['flavour'] == 'vanilla'
        assert unique_code['unique_code'] == 'vanilla0'
        self.assert_unique_code_counts(pool, [('vanilla', True, 1)])

        # Repeat the same request with the code already used.
        unique_code = self.successResultOf(
            pool.redeem_unique_code('vanilla0', audit_params))
        assert unique_code['flavour'] == 'vanilla'
        assert unique_code['unique_code'] == 'vanilla0'
        self.assert_unique_code_counts(pool, [('vanilla', True, 1)])

        # Reuse the same request_id but with different parameters.
        audit_params_2 = mk_audit_params('req-0', transaction_id='foo')
        self.failureResultOf(
            pool.redeem_unique_code('vanilla1', audit_params_2), AuditMismatch)

    def test_redeem_invalid_unique_code_idempotent(self):
        pool = UniqueCodePool('testpool', self.conn)
        self.successResultOf(pool.create_tables())
        self.assert_unique_code_counts(pool, [])

        # Attempt to redeem an invalid code.
        failure = self.failureResultOf(
            pool.redeem_unique_code('vanilla0', mk_audit_params('req-0')),
            CannotRedeemUniqueCode)
        assert failure.value.reason == 'invalid'
        assert failure.value.unique_code == 'vanilla0'

        # Repeat the same request with the code added to the pool.
        populate_pool(pool, ['vanilla'], [0])
        self.assert_unique_code_counts(pool, [('vanilla', False, 1)])
        failure = self.failureResultOf(
            pool.redeem_unique_code('vanilla0', mk_audit_params('req-0')),
            CannotRedeemUniqueCode)
        assert failure.value.reason == 'invalid'
        assert failure.value.unique_code == 'vanilla0'

    def test_query_by_request_id(self):
        pool = UniqueCodePool('testpool', self.conn)
        self.successResultOf(pool.create_tables())

        audit_params = mk_audit_params('req-0')
        rows = self.successResultOf(pool.query_by_request_id('req-0'))
        assert rows == []

        before = datetime.utcnow()
        self.successResultOf(pool._audit_request(
            audit_params, 'req_data', 'resp_data', 'vanilla0'))
        self.successResultOf(pool._audit_request(
            mk_audit_params('req-excl'), 'excl', 'excl', 'excl'))
        after = datetime.utcnow()

        rows = self.successResultOf(pool.query_by_request_id('req-0'))

        created_at = rows[0]['created_at']
        assert round_dt(before) <= round_dt(created_at) <= round_dt(after)
        assert rows == [{
            'request_id': audit_params['request_id'],
            'transaction_id': audit_params['transaction_id'],
            'user_id': audit_params['user_id'],
            'request_data': u'req_data',
            'response_data': u'resp_data',
            'error': False,
            'created_at': created_at,
        }]

    def test_query_by_transaction_id(self):
        pool = UniqueCodePool('testpool', self.conn)
        self.successResultOf(pool.create_tables())

        audit_params_0 = mk_audit_params('req-0', 'transaction-0')
        audit_params_1 = mk_audit_params('req-1', 'transaction-0')
        rows = self.successResultOf(
            pool.query_by_transaction_id('transaction-0'))
        assert rows == []

        before = round_dt(datetime.utcnow())
        self.successResultOf(pool._audit_request(
            audit_params_0, 'req_data_0', 'resp_data_0', 'vanilla0'))
        self.successResultOf(pool._audit_request(
            audit_params_1, 'req_data_1', 'resp_data_1', 'vanilla1'))
        self.successResultOf(pool._audit_request(
            mk_audit_params('req-excl'), 'excl', 'excl', 'excl'))
        after = round_dt(datetime.utcnow())

        rows = self.successResultOf(
            pool.query_by_transaction_id('transaction-0'))
        created_0 = rows[0]['created_at']
        created_1 = rows[1]['created_at']
        assert before <= round_dt(created_0) <= round_dt(created_1) <= after

        assert rows == [{
            'request_id': audit_params_0['request_id'],
            'transaction_id': audit_params_0['transaction_id'],
            'user_id': audit_params_0['user_id'],
            'request_data': u'req_data_0',
            'response_data': u'resp_data_0',
            'error': False,
            'created_at': created_0,
        }, {
            'request_id': audit_params_1['request_id'],
            'transaction_id': audit_params_1['transaction_id'],
            'user_id': audit_params_1['user_id'],
            'request_data': u'req_data_1',
            'response_data': u'resp_data_1',
            'error': False,
            'created_at': created_1,
        }]

    def test_query_by_user_id(self):
        pool = UniqueCodePool('testpool', self.conn)
        self.successResultOf(pool.create_tables())

        audit_params_0 = mk_audit_params('req-0', 'transaction-0', 'user-0')
        audit_params_1 = mk_audit_params('req-1', 'transaction-1', 'user-0')
        rows = self.successResultOf(pool.query_by_user_id('user-0'))
        assert rows == []

        before = round_dt(datetime.utcnow())
        self.successResultOf(pool._audit_request(
            audit_params_0, 'req_data_0', 'resp_data_0', 'vanilla0'))
        self.successResultOf(pool._audit_request(
            audit_params_1, 'req_data_1', 'resp_data_1', 'vanilla1'))
        self.successResultOf(pool._audit_request(
            mk_audit_params('req-excl'), 'excl', 'excl', 'excl'))
        after = round_dt(datetime.utcnow())

        rows = self.successResultOf(pool.query_by_user_id('user-0'))
        created_0 = rows[0]['created_at']
        created_1 = rows[1]['created_at']
        assert before <= round_dt(created_0) <= round_dt(created_1) <= after

        assert rows == [{
            'request_id': audit_params_0['request_id'],
            'transaction_id': audit_params_0['transaction_id'],
            'user_id': audit_params_0['user_id'],
            'request_data': u'req_data_0',
            'response_data': u'resp_data_0',
            'error': False,
            'created_at': created_0,
        }, {
            'request_id': audit_params_1['request_id'],
            'transaction_id': audit_params_1['transaction_id'],
            'user_id': audit_params_1['user_id'],
            'request_data': u'req_data_1',
            'response_data': u'resp_data_1',
            'error': False,
            'created_at': created_1,
        }]

    def test_query_by_unique_code(self):
        pool = UniqueCodePool('testpool', self.conn)
        self.successResultOf(pool.create_tables())
        populate_pool(pool, ['vanilla'], [0])

        audit_params = mk_audit_params('req-0')
        rows = self.successResultOf(pool.query_by_user_id('user-0'))
        assert rows == []

        before = datetime.utcnow()
        self.successResultOf(pool._audit_request(
            audit_params, 'req_data', 'resp_data', 'vanilla0'))
        self.successResultOf(pool._audit_request(
            mk_audit_params('req-excl'), 'excl', 'excl', 'excl'))
        after = datetime.utcnow()

        rows = self.successResultOf(pool.query_by_unique_code('vanilla0'))
        created_at = rows[0]['created_at']
        assert round_dt(before) <= round_dt(created_at) <= round_dt(after)

        assert rows == [{
            'request_id': audit_params['request_id'],
            'transaction_id': audit_params['transaction_id'],
            'user_id': audit_params['user_id'],
            'request_data': u'req_data',
            'response_data': u'resp_data',
            'error': False,
            'created_at': created_at,
        }]
