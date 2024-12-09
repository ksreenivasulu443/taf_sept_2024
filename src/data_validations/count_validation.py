def count_check(source, target):
    src_count = source.count()
    tgt_count = target.count()
    if src_count == tgt_count:
        status = 'PASS'
    else:
        status = 'FAIL'
    return status

