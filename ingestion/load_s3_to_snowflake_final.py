import boto3
import pandas as pd
import snowflake.connector

# ----------------------------
# S3 Configuration
# ----------------------------
bucket_name = "my-data-pipeline-raw-vignesh"
file_key = "bike_sales.csv"   # exact file name in S3

# ----------------------------
# Snowflake Configuration
# ----------------------------
sf_user = "8888"
sf_password = "Vignesh"        # <-- your Snowflake password
sf_account = "SGIVFZX"               # Snowflake account identifier only
sf_warehouse = "MY_WH"               # exact name of your warehouse in Snowflake
sf_role = "ACCOUNTADMIN"
sf_database = "MY_DATA_PIPELINE"
sf_schema = "RAW"
table_name = f"{sf_database}.{sf_schema}.stg_sales"  # fully qualified table name

# ----------------------------
# Step 1: Download CSV from S3
# ----------------------------
s3 = boto3.client('s3')
obj = s3.get_object(Bucket=bucket_name, Key=file_key)
data = pd.read_csv(obj['Body'])

# Strip extra spaces from column names
data.columns = data.columns.str.strip()

# Clean numeric columns
for col in ["Profit", "Cost", "Revenue"]:
    data[col] = data[col].replace({r'\$': '', ',': ''}, regex=True).astype(float)

# Convert Date column to Python datetime
data["Date"] = pd.to_datetime(data["Date"], format="%m/%d/%Y").dt.to_pydatetime()

print("CSV downloaded and cleaned!")

# ----------------------------
# Step 2: Connect to Snowflake
# ----------------------------
conn = snowflake.connector.connect(
    user=sf_user,
    password=sf_password,
    account=sf_account,
    role=sf_role
)
cs = conn.cursor()
print("Connected to Snowflake!")

# ----------------------------
# Step 3: Set warehouse, database, schema explicitly
# ----------------------------
# Quote warehouse to preserve exact case
cs.execute(f'USE WAREHOUSE "{sf_warehouse}"')

# Ensure database exists and is used
cs.execute(f"CREATE DATABASE IF NOT EXISTS {sf_database}")
cs.execute(f"USE DATABASE {sf_database}")

# Ensure schema exists and is used
cs.execute(f"CREATE SCHEMA IF NOT EXISTS {sf_database}.{sf_schema}")
cs.execute(f"USE SCHEMA {sf_schema}")

print(f"Warehouse, database, and schema are ready!")

# ----------------------------
# Step 4: Create staging table
# ----------------------------
cs.execute(f"""
CREATE TABLE IF NOT EXISTS {table_name} (
    Date DATE,
    Month STRING,
    Year INT,
    Customer_Age INT,
    Age_Group STRING,
    Customer_Gender STRING,
    Country STRING,
    State STRING,
    Product_Category STRING,
    Order_Quantity INT,
    Profit FLOAT,
    Cost FLOAT,
    Revenue FLOAT
)
""")
print(f"Staging table {table_name} ready!")

# ----------------------------
# Step 5: Insert data into Snowflake
# ----------------------------
for i, row in data.iterrows():
    cs.execute(f"""
        INSERT INTO {table_name} (
            Date, Month, Year, Customer_Age, Age_Group, Customer_Gender,
            Country, State, Product_Category, Order_Quantity, Profit, Cost, Revenue
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, (
        row.Date, row.Month, row.Year, row.Customer_Age, row.Age_Group, row.Customer_Gender,
        row.Country, row.State, row.Product_Category, row.Order_Quantity, row.Profit, row.Cost, row.Revenue
    ))

cs.close()
conn.close()
print("Data loaded into Snowflake successfully!")
