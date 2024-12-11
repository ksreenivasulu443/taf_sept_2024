import datetime

# Create a single report filename for the session
timestamp = datetime.datetime.now().strftime("%d%m%Y%H%M%S")
report_filename = f"report_{timestamp}.txt"

def write_output(validation_type, status, details):
    # Write the output to the report file
    with open(report_filename, "a") as report:
        report.write(f"{validation_type}: {status}\nDetails: {details}\n\n")


