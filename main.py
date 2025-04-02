import csv
from faker import Faker
from random import randint, choice
from datetime import datetime

# Initialize Faker instance
fake = Faker()


# Function to calculate age from birth_date
def calculate_age(birth_date):
    today = datetime.today()
    age = today.year - birth_date.year - ((today.month, today.day) < (birth_date.month, birth_date.day))
    return age


# Function to calculate years spent in the organization from joining_date
def calculate_years_spent(joining_date):
    today = datetime.today()
    years_spent = today.year - joining_date.year - ((today.month, today.day) < (joining_date.month, joining_date.day))
    return years_spent


# Function to calculate total salary (salary + bonus)
def calculate_total_salary(salary, bonus):
    return salary + bonus


# Function to determine employee type based on years_spent_in_org
def determine_employee_type(years_spent):
    return 'Experienced' if years_spent >= 3 else 'Fresher'


# Function to generate department data
def generate_dept_data():
    dept_data = {
        'D001': 'HR',
        'D002': 'IT',
        'D003': 'Finance',
        'D004': 'Sales'
    }
    dept_id = choice(list(dept_data.keys()))
    dept_name = dept_data[dept_id]
    return dept_id, dept_name


# Function to generate employee data
def generate_employee_data():
    eid = fake.unique.uuid4()
    emp_name = fake.name()
    birth_date = fake.date_of_birth(minimum_age=18, maximum_age=60)
    joining_date = fake.date_this_decade(before_today=True, after_today=False)
    salary = randint(40000, 70000)
    bonus = randint(1000, 10000)
    dept_id, dept_name = generate_dept_data()

    # Calculations based on mappings
    age = calculate_age(birth_date)
    years_spent_in_org = calculate_years_spent(joining_date)
    total_salary = calculate_total_salary(salary, bonus)
    employee_type = determine_employee_type(years_spent_in_org)

    # Return data as a dictionary
    return {
        "eid": eid,
        "emp_name": emp_name,
        "birth_date": birth_date.strftime('%Y-%m-%d'),
        "joining_date": joining_date.strftime('%Y-%m-%d'),
        "salary": salary,
        "bonus": bonus,
        "dept_id": dept_id,
        "dept_name": dept_name,
        "age": age,
        "years_spent_in_org": years_spent_in_org,
        "total_salary": total_salary,
        "employee_type": employee_type
    }


# Function to generate a CSV file with the specified number of rows
def generate_csv(num_records):
    # Generate the employee data
    employee_data = [generate_employee_data() for _ in range(num_records)]

    # Get the current date to format the filename
    current_date = datetime.today().strftime('%Y%m%d')

    # Define the file name based on customer and batch date
    file_name = f"customer_{current_date}.csv"

    # Define the CSV fieldnames (columns)
    fieldnames = ["eid", "emp_name", "birth_date", "joining_date", "salary", "bonus", "dept_id", "dept_name", "age",
                  "years_spent_in_org", "total_salary", "employee_type"]

    # Write the data to a CSV file
    with open(file_name, mode='w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)

        # Write the header
        writer.writeheader()

        # Write employee data
        for employee in employee_data:
            writer.writerow(employee)

    print(f"Test data has been written to {file_name}")


# Prompt for the number of records
try:
    num_records = int(input("Enter the number of test records to generate: "))
    generate_csv(num_records)
except ValueError:
    print("Please enter a valid number.")
