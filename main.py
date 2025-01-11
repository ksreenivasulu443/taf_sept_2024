import streamlit as st
import pandas as pd
from faker import Faker
import random
import string
import os

# Initialize Faker
fake = Faker()

# Helper function to generate data for a single column
from datetime import datetime

def generate_column_data(col_name, col_type, row_count, constraints):
    data = []
    for _ in range(row_count):
        if col_type == 'integer':
            data.append(random.randint(constraints.get('min', 0), constraints.get('max', 100)))
        elif col_type == 'float':
            data.append(random.uniform(constraints.get('min', 0.0), constraints.get('max', 100.0)))
        elif col_type == 'string':
            length = constraints.get('length', 10)
            data.append(''.join(random.choices(string.ascii_letters + string.digits, k=length)))
        elif col_type == 'date':
            start_date_str = constraints.get('start_date', '2000-01-01')
            end_date_str = constraints.get('end_date', '2030-01-01')
            start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
            end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date()
            data.append(fake.date_between(start_date=start_date, end_date=end_date))
        elif col_type == 'boolean':
            data.append(random.choice([True, False]))
        else:
            data.append(None)
    return data

# Helper function to generate dataset
def generate_dataset(columns, row_count):
    data = {}
    for index, row in columns.iterrows():
        col_name = row['Column Name']
        col_type = row['Data Type']
        constraints = row['Constraints'] if isinstance(row['Constraints'], dict) else {}
        data[col_name] = generate_column_data(col_name, col_type, row_count, constraints)
    return pd.DataFrame(data)

# Streamlit UI
st.title("Test Data Generator")

# File upload
uploaded_file = st.file_uploader("Upload a CSV or Excel file with column names and data types", type=["csv", "xlsx"])

if uploaded_file:
    if uploaded_file.name.endswith('csv'):
        columns_df = pd.read_csv(uploaded_file)
    else:
        columns_df = pd.read_excel(uploaded_file)

    st.write("### Uploaded Column Details")
    st.write(columns_df)

    # Input for number of records
    row_count = st.number_input("Number of Records to Generate", min_value=1, step=1)

    # Generate Data Button
    if st.button("Generate Test Data"):
        test_data = generate_dataset(columns_df, row_count)
        st.write("### Generated Test Data")
        st.write(test_data)

        # Export options
        export_format = st.selectbox("Select Export Format", ["CSV", "JSON", "Parquet"])
        if st.button("Export Data"):
            output_path = "generated_data." + export_format.lower()
            if export_format == "CSV":
                test_data.to_csv(output_path, index=False)
            elif export_format == "JSON":
                test_data.to_json(output_path, orient="records", lines=True)
            elif export_format == "Parquet":
                test_data.to_parquet(output_path, index=False)

            st.success(f"Data exported successfully to {output_path}")
            with open(output_path, "rb") as file:
                st.download_button(label="Download File", data=file, file_name=output_path)

