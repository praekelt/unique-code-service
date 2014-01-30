from datetime import datetime
from hashlib import md5
from itertools import izip_longest
import json
from urllib import urlencode
from StringIO import StringIO

from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks
from twisted.trial.unittest import TestCase
from twisted.web.client import Agent, FileBodyProducer, readBody
from twisted.web.http_headers import Headers
from twisted.web.server import Site

from unique_code_service.api import UniqueCodeServiceApp
from unique_code_service.models import UniqueCodePool

from .helpers import populate_pool, mk_audit_params, sorted_dicts


class ApiClient(object):
    def __init__(self, base_url):
        self._base_url = base_url

    def _make_url(self, url_path):
        return '%s/%s' % (self._base_url, url_path.lstrip('/'))

    def _make_call(self, method, url_path, headers, body, expected_code):
        agent = Agent(reactor)
        url = self._make_url(url_path)
        d = agent.request(method, url, headers, body)
        return d.addCallback(self._get_response_body, expected_code)

    def _get_response_body(self, response, expected_code):
        assert response.code == expected_code
        return readBody(response).addCallback(json.loads)

    def get(self, url_path, params, expected_code):
        url_path = '?'.join([url_path, urlencode(params)])
        return self._make_call('GET', url_path, None, None, expected_code)

    def put(self, url_path, headers, content, expected_code=200):
        body = FileBodyProducer(StringIO(content))
        return self._make_call('PUT', url_path, headers, body, expected_code)

    def put_json(self, url_path, params, expected_code=200):
        headers = Headers({'Content-Type': ['application/json']})
        return self.put(
            url_path, headers, json.dumps(params), expected_code)

    def put_redeem(self, request_id, unique_code, expected_code=200):
        params = mk_audit_params(request_id)
        params.update({
            'unique_code': unique_code,
        })
        params.pop('request_id')
        url_path = 'testpool/redeem/%s' % (request_id,)
        return self.put_json(url_path, params, expected_code)

    def put_create(self, expected_code=201):
        url_path = 'testpool'
        return self.put(url_path, Headers({}), None, expected_code)

    def put_import(self, request_id, content, content_md5=None,
                   expected_code=201):
        url_path = 'testpool/import/%s' % (request_id,)
        hdict = {
            'Content-Type': ['text/csv'],
        }
        if content_md5 is None:
            content_md5 = md5(content).hexdigest()
        if content_md5:
            hdict['Content-MD5'] = [content_md5]
        return self.put(url_path, Headers(hdict), content, expected_code)

    def get_audit_query(self, request_id, field, value, expected_code=200):
        params = {'request_id': request_id, 'field': field, 'value': value}
        return self.get('testpool/audit_query', params, expected_code)

    def get_unique_code_counts(self, request_id, expected_code=200):
        params = {'request_id': request_id}
        return self.get('testpool/unique_code_counts', params, expected_code)


class TestUniqueCodeServiceApp(TestCase):
    timeout = 5

    @inlineCallbacks
    def setUp(self):
        # We need to make sure all our queries run in the same thread,
        # otherwise sqlite gets very sad.
        reactor.suggestThreadPoolSize(1)
        self.asapp = UniqueCodeServiceApp("sqlite://", reactor=reactor)
        site = Site(self.asapp.app.resource())
        self.listener = reactor.listenTCP(0, site, interface='localhost')
        self.listener_port = self.listener.getHost().port
        self.conn = yield self.asapp.engine.connect()
        self.pool = UniqueCodePool('testpool', self.conn)
        self.client = ApiClient('http://localhost:%s' % (self.listener_port,))

    def tearDown(self):
        return self.listener.loseConnection()

    @inlineCallbacks
    def assert_unique_code_counts(self, expected_rows):
        rows = yield self.pool.count_unique_codes()
        assert sorted(tuple(r) for r in rows) == sorted(expected_rows)

    @inlineCallbacks
    def test_request_missing_params(self):
        params = mk_audit_params('req-0')
        params.pop('request_id')
        rsp = yield self.client.put_json(
            'testpool/redeem/req-0', params, expected_code=400)
        assert rsp == {
            'request_id': 'req-0',
            'error': "Missing request parameters: 'unique_code'",
        }

    @inlineCallbacks
    def test_request_missing_audit_params(self):
        params = {'unique_code': 'vanilla0'}
        rsp = yield self.client.put_json(
            'testpool/redeem/req-0', params, expected_code=400)
        assert rsp == {
            'request_id': 'req-0',
            'error': (
                "Missing request parameters: 'transaction_id', 'user_id'"),
        }

    @inlineCallbacks
    def test_request_extra_params(self):
        params = mk_audit_params('req-0')
        params.pop('request_id')
        params.update({
            'unique_code': 'vanilla0',
            'foo': 'bar',
        })
        rsp = yield self.client.put_json(
            'testpool/redeem/req-0', params, expected_code=400)
        assert rsp == {
            'request_id': 'req-0',
            'error': "Unexpected request parameters: 'foo'",
        }

    @inlineCallbacks
    def test_redeem_missing_pool(self):
        rsp = yield self.client.put_redeem(
            'req-0', 'vanilla0', expected_code=404)
        assert rsp == {
            'request_id': 'req-0',
            'error': 'Unique code pool does not exist.',
        }

    @inlineCallbacks
    def test_issue_response_contains_request_id(self):
        yield self.pool.create_tables()
        yield populate_pool(self.pool, ['vanilla'], [0])
        rsp0 = yield self.client.put_redeem('req-0', 'vanilla0')
        assert rsp0['request_id'] == 'req-0'

    @inlineCallbacks
    def test_redeem(self):
        yield self.pool.create_tables()
        yield populate_pool(self.pool, ['vanilla', 'chocolate'], [0, 1])
        rsp0 = yield self.client.put_redeem('req-0', 'vanilla0')
        assert rsp0 == {
            'request_id': 'req-0',
            'unique_code': 'vanilla0',
            'flavour': 'vanilla',
        }

        rsp1 = yield self.client.put_redeem('req-1', 'chocolate1')
        assert rsp1 == {
            'request_id': 'req-1',
            'unique_code': 'chocolate1',
            'flavour': 'chocolate',
        }

    @inlineCallbacks
    def test_redeem_idempotent(self):
        yield self.pool.create_tables()
        yield populate_pool(self.pool, ['vanilla'], [0])
        rsp0 = yield self.client.put_redeem('req-0', 'vanilla0')
        assert rsp0 == {
            'request_id': 'req-0',
            'unique_code': 'vanilla0',
            'flavour': 'vanilla',
        }

        rsp1 = yield self.client.put_redeem('req-0', 'vanilla0')
        assert rsp1 == {
            'request_id': 'req-0',
            'unique_code': 'vanilla0',
            'flavour': 'vanilla',
        }

        rsp2 = yield self.client.put_redeem('req-1', 'vanilla0')
        assert rsp2 == {
            'request_id': 'req-1',
            'error': 'Cannot redeem unique code: used',
        }

        rsp3 = yield self.client.put_redeem(
            'req-0', 'VaNiLlA0', expected_code=400)
        assert rsp3 == {
            'request_id': 'req-0',
            'error': (
                'This request has already been performed with different'
                ' parameters.'),
        }

    @inlineCallbacks
    def test_redeem_invalid_unique_code(self):
        yield self.pool.create_tables()
        yield populate_pool(self.pool, ['vanilla'], [0])
        rsp = yield self.client.put_redeem('req-0', 'chocolate7')
        assert rsp == {
            'request_id': 'req-0',
            'error': 'Cannot redeem unique code: invalid',
        }

    @inlineCallbacks
    def test_redeem_used_unique_code(self):
        yield self.pool.create_tables()
        yield populate_pool(self.pool, ['vanilla'], [0])
        yield self.client.put_redeem('req-0', 'vanilla0')
        rsp = yield self.client.put_redeem('req-1', 'vanilla0')
        assert rsp == {
            'request_id': 'req-1',
            'error': 'Cannot redeem unique code: used',
        }

    def _assert_audit_entries(self, request_id, response, expected_entries):
        def created_ats():
            for result in response['results']:
                yield datetime.strptime(
                    result['created_at'], '%Y-%m-%dT%H:%M:%S.%f').isoformat()

        expected_results = [{
            'request_id': entry['audit_params']['request_id'],
            'transaction_id': entry['audit_params']['transaction_id'],
            'user_id': entry['audit_params']['user_id'],
            'request_data': entry['request_data'],
            'response_data': entry['response_data'],
            'error': entry['error'],
            'created_at': created_at,
        } for entry, created_at in izip_longest(
            expected_entries, created_ats())]

        assert response == {
            'request_id': request_id,
            'results': expected_results,
        }

    @inlineCallbacks
    def test_query_bad_field(self):
        yield self.pool.create_tables()

        rsp = yield self.client.get_audit_query(
            'audit-0', 'foo', 'req-0', expected_code=400)
        assert rsp == {
            'request_id': 'audit-0',
            'error': 'Invalid audit field.',
        }

    @inlineCallbacks
    def test_query_by_request_id(self):
        yield self.pool.create_tables()

        audit_params = mk_audit_params('req-0')
        rsp = yield self.client.get_audit_query(
            'audit-0', 'request_id', 'req-0')
        assert rsp == {
            'request_id': 'audit-0',
            'results': [],
        }

        yield self.pool._audit_request(
            audit_params, 'req_data', 'resp_data', 'vanilla0')

        rsp = yield self.client.get_audit_query(
            'audit-1', 'request_id', 'req-0')

        self._assert_audit_entries('audit-1', rsp, [{
            'audit_params': audit_params,
            'request_data': u'req_data',
            'response_data': u'resp_data',
            'error': False,
        }])

    @inlineCallbacks
    def test_query_by_transaction_id(self):
        yield self.pool.create_tables()

        audit_params_0 = mk_audit_params('req-0', 'transaction-0')
        audit_params_1 = mk_audit_params('req-1', 'transaction-0')
        rsp = yield self.client.get_audit_query(
            'audit-0', 'transaction_id', 'transaction-0')
        assert rsp == {
            'request_id': 'audit-0',
            'results': [],
        }

        yield self.pool._audit_request(
            audit_params_0, 'req_data_0', 'resp_data_0', 'vanilla0')
        yield self.pool._audit_request(
            audit_params_1, 'req_data_1', 'resp_data_1', 'vanilla0')

        rsp = yield self.client.get_audit_query(
            'audit-1', 'transaction_id', 'transaction-0')

        self._assert_audit_entries('audit-1', rsp, [{
            'audit_params': audit_params_0,
            'request_data': u'req_data_0',
            'response_data': u'resp_data_0',
            'error': False,
        }, {
            'audit_params': audit_params_1,
            'request_data': u'req_data_1',
            'response_data': u'resp_data_1',
            'error': False,
        }])

    @inlineCallbacks
    def test_query_by_user_id(self):
        yield self.pool.create_tables()

        audit_params_0 = mk_audit_params('req-0', 'transaction-0', 'user-0')
        audit_params_1 = mk_audit_params('req-1', 'transaction-1', 'user-0')
        rsp = yield self.client.get_audit_query('audit-0', 'user_id', 'user-0')
        assert rsp == {
            'request_id': 'audit-0',
            'results': [],
        }

        yield self.pool._audit_request(
            audit_params_0, 'req_data_0', 'resp_data_0', 'vanilla0')
        yield self.pool._audit_request(
            audit_params_1, 'req_data_1', 'resp_data_1', 'vanilla0')

        rsp = yield self.client.get_audit_query('audit-1', 'user_id', 'user-0')

        self._assert_audit_entries('audit-1', rsp, [{
            'audit_params': audit_params_0,
            'request_data': u'req_data_0',
            'response_data': u'resp_data_0',
            'error': False,
        }, {
            'audit_params': audit_params_1,
            'request_data': u'req_data_1',
            'response_data': u'resp_data_1',
            'error': False,
        }])

    @inlineCallbacks
    def test_query_by_unique_code(self):
        yield self.pool.create_tables()

        audit_params_0 = mk_audit_params('req-0', 'transaction-0', 'user-0')
        audit_params_1 = mk_audit_params('req-1', 'transaction-1', 'user-0')
        rsp = yield self.client.get_audit_query('audit-0', 'user_id', 'user-0')
        assert rsp == {
            'request_id': 'audit-0',
            'results': [],
        }

        yield self.pool._audit_request(
            audit_params_0, 'req_data_0', 'resp_data_0', 'vanilla0')
        yield self.pool._audit_request(
            audit_params_1, 'req_data_1', 'resp_data_1', 'vanilla0')

        rsp = yield self.client.get_audit_query(
            'audit-1', 'unique_code', 'vanilla0')

        self._assert_audit_entries('audit-1', rsp, [{
            'audit_params': audit_params_0,
            'request_data': u'req_data_0',
            'response_data': u'resp_data_0',
            'error': False,
        }, {
            'audit_params': audit_params_1,
            'request_data': u'req_data_1',
            'response_data': u'resp_data_1',
            'error': False,
        }])

    @inlineCallbacks
    def test_create(self):
        resp = yield self.client.put_create()
        assert resp == {
            'request_id': None,
            'created': True,
        }
        # Recreating a pool has a different response.
        resp = yield self.client.put_create(expected_code=200)
        assert resp == {
            'request_id': None,
            'created': False,
        }

    @inlineCallbacks
    def test_import(self):
        yield self.pool.create_tables()
        yield self.assert_unique_code_counts([])

        content = '\n'.join([
            'unique_code,flavour',
            'vanilla0,vanilla',
            'vanilla1,vanilla',
            'chocolate0,chocolate',
            'chocolate1,chocolate',
        ])

        resp = yield self.client.put_import('req-0', content)
        assert resp == {
            'request_id': 'req-0',
            'imported': True,
        }
        yield self.assert_unique_code_counts([
            ('vanilla', False, 2),
            ('chocolate', False, 2),
        ])

    @inlineCallbacks
    def test_import_missing_pool(self):
        content = '\n'.join([
            'unique_code,flavour',
            'vanilla0,vanilla',
            'vanilla1,vanilla',
            'chocolate0,chocolate',
            'chocolate1,chocolate',
        ])

        rsp = yield self.client.put_import('req-0', content, expected_code=404)
        assert rsp == {
            'request_id': 'req-0',
            'error': 'Unique code pool does not exist.',
        }

    @inlineCallbacks
    def test_import_heading_case_mismatch(self):
        yield self.pool.create_tables()
        yield self.assert_unique_code_counts([])

        content = '\n'.join([
            'Unique_cOdE,fLavoUr',
            'vanilla0,vanilla',
            'vanilla1,vanilla',
            'chocolate0,chocolate',
            'chocolate1,chocolate',
        ])

        resp = yield self.client.put_import('req-0', content)
        assert resp == {
            'request_id': 'req-0',
            'imported': True,
        }
        yield self.assert_unique_code_counts([
            ('vanilla', False, 2),
            ('chocolate', False, 2),
        ])

    @inlineCallbacks
    def test_import_no_content_md5(self):
        yield self.pool.create_tables()

        resp = yield self.client.put_import(
            'req-0', 'content', '', expected_code=400)
        assert resp == {
            'request_id': 'req-0',
            'error': 'Missing Content-MD5 header.',
        }

    @inlineCallbacks
    def test_import_bad_content_md5(self):
        yield self.pool.create_tables()

        resp = yield self.client.put_import(
            'req-0', 'content', 'badmd5', expected_code=400)
        assert resp == {
            'request_id': 'req-0',
            'error': 'Content-MD5 header does not match content.',
        }

    @inlineCallbacks
    def test_import_idempotent(self):
        yield self.pool.create_tables()
        yield self.assert_unique_code_counts([])

        content = '\n'.join([
            'unique_code,flavour',
            'vanilla0,vanilla',
            'vanilla1,vanilla',
            'chocolate0,chocolate',
            'chocolate1,chocolate',
        ])

        expected_counts = [
            ('vanilla', False, 2),
            ('chocolate', False, 2),
        ]

        resp = yield self.client.put_import('req-0', content)
        assert resp == {
            'request_id': 'req-0',
            'imported': True,
        }
        yield self.assert_unique_code_counts(expected_counts)

        resp = yield self.client.put_import('req-0', content)
        assert resp == {
            'request_id': 'req-0',
            'imported': True,
        }
        yield self.assert_unique_code_counts(expected_counts)

        content_2 = '\n'.join([
            'unique_code,flavour',
            'vanilla6,vanilla',
            'vanilla7,vanilla',
            'chocolate8,chocolate',
            'chocolate9,chocolate',
        ])

        resp = yield self.client.put_import(
            'req-0', content_2, expected_code=400)
        assert resp == {
            'request_id': 'req-0',
            'error': (
                'This request has already been performed with different'
                ' parameters.'),
        }
        yield self.assert_unique_code_counts(expected_counts)

    @inlineCallbacks
    def test_unique_code_counts(self):
        yield self.pool.create_tables()
        rsp0 = yield self.client.get_unique_code_counts('req-0')
        assert rsp0 == {
            'request_id': 'req-0',
            'unique_code_counts': [],
        }

        yield populate_pool(self.pool, ['vanilla'], [0, 1])
        rsp1 = yield self.client.get_unique_code_counts('req-1')
        assert rsp1 == {
            'request_id': 'req-1',
            'unique_code_counts': [
                {
                    'flavour': 'vanilla',
                    'used': False,
                    'count': 2,
                },
            ],
        }

        yield populate_pool(self.pool, ['chocolate'], [0, 1])
        yield self.pool.redeem_unique_code(
            'chocolate0', mk_audit_params('req-0'))
        rsp2 = yield self.client.get_unique_code_counts('req-2')
        assert sorted_dicts(rsp2['unique_code_counts']) == sorted_dicts([
            {
                'flavour': 'vanilla',
                'used': False,
                'count': 2,
            },
            {
                'flavour': 'chocolate',
                'used': False,
                'count': 1,
            },
            {
                'flavour': 'chocolate',
                'used': True,
                'count': 1,
            },
        ])
