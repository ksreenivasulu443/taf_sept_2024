def count_check(source, target):
    src_count = source.count()
    tgt_count = target.count()
    print(f"source count is {src_count} and target count is {tgt_count}")
    if src_count == tgt_count:
        status = 'PASS'
    else:
        status = 'FAIL'
    return status

