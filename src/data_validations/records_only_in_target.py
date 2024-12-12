from src.utility.report_lib import write_output

def records_only_in_target(source_df, target_df, config):
    """Validate records present only in the target."""
    key_columns = config.get("key_columns", [])
    only_in_target = target_df.select(key_columns).exceptAll(source_df.select(key_columns))

    count_only_in_target = only_in_target.count()
    if count_only_in_target > 0:
        failed_records = only_in_target.limit(5).collect()  # Get the first 5 failing rows
        failed_preview = [row.asDict() for row in failed_records]  # Convert rows to a dictionary for display
        status = "FAIL"
        write_output(
            "Records Only in Target",
            status,
            f"Count: {count_only_in_target}, Sample Failed Records: {failed_preview}"
        )
        return False
    else:
        status = "PASS"
        write_output("Records Only in Target", status, "No extra records found in target.")
        return True
