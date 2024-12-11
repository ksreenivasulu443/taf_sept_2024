import datetime

# Create a single report filename for the session
timestamp = datetime.datetime.now().strftime("%d%m%Y%H%M%S")
print("datetime.datetime.now()",datetime.datetime.now())
report_filename = f"report_{timestamp}.txt"

print(report_filename)