from uuid import uuid4


def populate_pool(pool, flavours, suffixes):
    return pool.import_unique_codes(str(uuid4()), 'md5', [
        {
            'flavour': flavour,
            'unique_code': '%s%s' % (flavour, suffix),
        }
        for flavour in flavours
        for suffix in suffixes
    ])


def mk_audit_params(request_id, transaction_id=None, user_id=None):
    if transaction_id is None:
        transaction_id = 'tx-%s' % (request_id,)
    if user_id is None:
        user_id = 'user-%s' % (transaction_id,)
    return {
        'request_id': request_id,
        'transaction_id': transaction_id,
        'user_id': user_id,
    }


def sorted_dicts(dicts):
    return sorted(dicts, key=lambda d: sorted(d.items()))
