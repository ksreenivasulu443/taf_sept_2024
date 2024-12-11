from src.utility.report_lib import write_output
def duplicate_check(target_df, key_col,config_data):
    dup = target_df.groupBy(key_col).count().filter('count>1')
    dup_count = dup.count()
    if dup_count>0:
        print("duplicates present, below are the sample duplicates")
        dup.show(5)
        status = 'FAIL'
        write_output(validation_type='duplicate_check',status=status,details=dup.show())

    else:
        print("duplicates are not present")
        status ='PASS'
        write_output(validation_type='duplicate_check', status=status, details=dup)
    return status


