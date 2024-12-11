def duplicate_check(target_df, key_col):
    # target_df.createOrReplacetempView('target_df')
    # dup = spark.sql("select key_column, count(1) from target_df group by key_column having count(1)>1")
    dup = target_df.groupBy(key_col).count().filter('count>1')
    dup_count = dup.count()
    if dup_count>0:
        print("duplicates present, below are the sample duplicates")
        dup.show(5)
        status = 'FAIL'
    else:
        print("duplicates are not present")
        status ='PASS'
    return status


