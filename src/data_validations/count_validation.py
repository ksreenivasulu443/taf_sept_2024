from src.utility.report_lib import write_output
from src.data_validations.records_only_in_source import records_only_in_source
from src.data_validations.records_only_in_target import records_only_in_target
def count_check(source, target,key_columns):
    src_count = source.count()
    tgt_count = target.count()
    print(f"source count is {src_count} and target count is {tgt_count}")
    if src_count == tgt_count:
        status = 'PASS'
        print(f"source count is {src_count} and target count is {tgt_count}")
        write_output(validation_type='count_check', status=status, details='count is matching!')
        # records_only_in_source(source,target,key_columns)
        # records_only_in_target(source,target,key_columns)
    else:
        status = 'FAIL'
        write_output(validation_type='count_check', status=status, details='details below...')
        records_only_in_source(source,target,key_columns)
        records_only_in_target(source,target,key_columns)

    return status
