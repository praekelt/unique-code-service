from StringIO import StringIO
import csv
from hashlib import md5

from aludel.database import get_engine
from aludel.service import (
    service, handler, get_url_params, get_json_params, set_request_id,
    APIError, BadRequestParams,
)

from twisted.internet.defer import inlineCallbacks, returnValue

from .models import (
    UniqueCodePool, CannotRedeemUniqueCode, NoUniqueCodePool, AuditMismatch,
)


@service
class UniqueCodeServiceApp(object):
    def __init__(self, conn_str, reactor):
        self.engine = get_engine(conn_str, reactor)

    def handle_api_error(self, failure, request):
        if failure.check(NoUniqueCodePool):
            raise APIError('Unique code pool does not exist.', 404)
        if failure.check(AuditMismatch):
            raise BadRequestParams(
                "This request has already been performed with different"
                " parameters.")
        return failure

    @handler(
        '/<string:unique_code_pool>/redeem/<string:request_id>',
        methods=['PUT'])
    @inlineCallbacks
    def redeem_unique_code(self, request, unique_code_pool, request_id):
        set_request_id(request, request_id)
        params = get_json_params(
            request, ['transaction_id', 'user_id', 'unique_code'])
        audit_params = {
            'request_id': request_id,
            'transaction_id': params.pop('transaction_id'),
            'user_id': params.pop('user_id'),
        }
        conn = yield self.engine.connect()
        pool = UniqueCodePool(unique_code_pool, conn)
        try:
            unique_code = yield pool.redeem_unique_code(
                params['unique_code'], audit_params)
        except CannotRedeemUniqueCode as e:
            # This is a normal condition, so we still return a 200 OK.
            raise APIError('Cannot redeem unique code: %s' % (e.reason,), 200)
        finally:
            yield conn.close()

        returnValue({
            'unique_code': unique_code['unique_code'],
            'flavour': unique_code['flavour'],
        })

    @handler('/<string:unique_code_pool>/audit_query', methods=['GET'])
    @inlineCallbacks
    def audit_query(self, request, unique_code_pool):
        params = get_url_params(
            request, ['field', 'value'], ['request_id'])
        if params['field'] not in [
                'request_id', 'transaction_id', 'user_id', 'unique_code']:
            raise BadRequestParams('Invalid audit field.')

        conn = yield self.engine.connect()
        pool = UniqueCodePool(unique_code_pool, conn)
        try:
            query = {
                'request_id': pool.query_by_request_id,
                'transaction_id': pool.query_by_transaction_id,
                'user_id': pool.query_by_user_id,
                'unique_code': pool.query_by_unique_code,
            }[params['field']]
            rows = yield query(params['value'])
        finally:
            yield conn.close()

        results = [{
            'request_id': row['request_id'],
            'transaction_id': row['transaction_id'],
            'user_id': row['user_id'],
            'request_data': row['request_data'],
            'response_data': row['response_data'],
            'error': row['error'],
            'created_at': row['created_at'].isoformat(),
        } for row in rows]
        returnValue({'results': results})

    @handler('/<string:unique_code_pool>', methods=['PUT'])
    @inlineCallbacks
    def create_pool(self, request, unique_code_pool):
        conn = yield self.engine.connect()
        pool = UniqueCodePool(unique_code_pool, conn)
        try:
            already_exists = yield pool.exists()
            if not already_exists:
                request.setResponseCode(201)
                yield pool.create_tables()
        finally:
            yield conn.close()

        returnValue({'created': not already_exists})

    @handler(
        '/<string:unique_code_pool>/import/<string:request_id>',
        methods=['PUT'])
    @inlineCallbacks
    def import_unique_codes(self, request, unique_code_pool, request_id):
        set_request_id(request, request_id)
        content_md5 = request.requestHeaders.getRawHeaders('Content-MD5')
        if content_md5 is None:
            raise BadRequestParams("Missing Content-MD5 header.")
        content_md5 = content_md5[0].lower()
        content = request.content.read()
        if content_md5 != md5(content).hexdigest().lower():
            raise BadRequestParams(
                "Content-MD5 header does not match content.")

        reader = csv.DictReader(StringIO(content))
        row_iter = lowercase_row_keys(reader)

        conn = yield self.engine.connect()
        pool = UniqueCodePool(unique_code_pool, conn)
        try:
            yield pool.import_unique_codes(request_id, content_md5, row_iter)
        finally:
            yield conn.close()

        request.setResponseCode(201)
        returnValue({'imported': True})

    @handler(
        '/<string:unique_code_pool>/unique_code_counts', methods=['GET'])
    @inlineCallbacks
    def unique_code_counts(self, request, unique_code_pool):
        # This sets the request_id on the request object.
        get_url_params(request, [], ['request_id'])

        conn = yield self.engine.connect()
        pool = UniqueCodePool(unique_code_pool, conn)
        try:
            rows = yield pool.count_unique_codes()
        finally:
            yield conn.close()

        results = [{
            'flavour': row['flavour'],
            'used': row['used'],
            'count': row['count'],
        } for row in rows]
        returnValue({'unique_code_counts': results})


def lowercase_row_keys(rows):
    for row in rows:
        yield dict((k.lower(), v) for k, v in row.iteritems())
