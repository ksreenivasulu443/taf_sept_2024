from src.utility.report_lib import write_output
def count_check(source, target):
    src_count = source.count()
    tgt_count = target.count()
    print(f"source count is {src_count} and target count is {tgt_count}")
    if src_count == tgt_count:
        status = 'PASS'
        write_output(validation_type='count_check', status=status, details='count')
    else:
        status = 'FAIL'
        write_output(validation_type='count_check', status=status, details='count')
    return status


