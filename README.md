# SQL-ETL-Pipeline

This project demonstrates an ETL pipeline that extracts data from multiple CSV files (containing customer, rental, address, city, country, payment, store, and staff information), performs transformations using Pandas to clean and merge the data, and then loads the results into a SQL database.

- Extracts data from multiple CSV files (customer.csv, rental.csv, address.csv, city.csv, country.csv, payment.csv, staff.csv, store.csv).
- Transforms the data by joining the tables on relevant keys (e.g., customer_id, address_id, city_id, country_id, etc.) using Pandas.
- Renames columns for consistency and clarity after the join operation.
- Drops duplicate and unnecessary columns to ensure a clean final dataset.
- Loads the cleaned and transformed data into a SQL database using to_sql().
- Executes SQL queries to display the first 10 rows, select specific columns, count rows, and filter based on conditions.
- Closes the database connection after the operations are completed.

## Technologies Used

- **VS Code**
- **Python**
- **Pandas**
- **Sqlite3**

## How To Run This Code On VS Code

1. Install Python and VS Code (if you haven't already):
   - **https://www.python.org/downloads/**
   - **https://code.visualstudio.com/**

3. Clone the repository to your local machine:
   ```bash
   git clone https://github.com/your-username/your-repository-name.git
4. Install the required dependencies by navigating to your project folder and running:   
   ```bash
   pip install -r requirements.txt
5. Open the project in VS Code:
   - **Launch VS Code and open the folder where you cloned the project.**
6. Open the Python script (e.g., etl_pipeline.py) in VS Code.
7. Run the script:
   - **Click on the Run button or press Ctrl + F5 to execute the ETL pipeline.**
8. Monitor the output:
   - **The script will extract data from the CSV files, transform it, and load the results into your database.**
   - **You can see the logs and results in the Terminal.**
9. SQL Queries:
    - **The script includes SQL queries to display and analyze the data after loading it into the database.**

## ETL Pipeline Overview

- **Extract**: Pulls CSV data fron file.
- **Transform**:
  - Join different CSV files.
  - Rename columns.
  - Drop repeated columns.
- **Load**: Loads cleaned data to database
## Run Queries
## Close the connection
