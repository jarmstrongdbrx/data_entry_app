# Configuration Editor
![s09593405062025](https://a.okmd.dev/md/681a15c939929.png)
This Streamlit app allows users to view, edit, and manage configuration tables stored in Delta Lake and cataloged with Unity Catalog. The app provides a modern, user-friendly interface for interacting with configuration data, supporting operations like adding, updating, and deleting rows in the tables.
## Features

- Dynamic Table Listing: Automatically fetches and displays all tables from the specified schema (john_armstrong.configurations).
- Editable Data Grid: Users can view and edit table data in a grid format, with support for adding new rows, updating existing ones, and deleting rows.
- Automatic Timestamp Handling: The app automatically manages CreatedAt and UpdatedAt timestamps, setting them to the current time during inserts and updates.
- Delete Functionality: Supports row deletion through a programmatic is_delete flag, ensuring seamless integration with the MERGE statement.
Primary Key Detection: Dynamically fetches primary keys for each table to ensure proper data merging.
- Modern UI: Uses expanders for a clean, card-like layout, with read-only timestamp columns and hidden technical fields.

## Installation

Clone the Repository: <br>
git clone https://github.com/jarmstrongdbrx/data_entry_app <br>
cd data_entry_app


## Set Up Environment:

Install the Databricks CLI -> https://docs.databricks.com/aws/en/dev-tools/cli/install

### Local
1. Ensure you have Python 3.8+ installed.
Install the required dependencies: <br>
`pip install -r requirements.txt`

2. Configure Environment Variables:

    Set the following environment variables
    - `DATABRICKS_WAREHOUSE_ID` environment variable to your Databricks SQL Warehouse ID.
    - `CATALOG` the catalog holding the schema & tables you want to be editable in the UI
    - `SCHEMA` the schema hold the tables you wnat to be editable in the UI
    - `USER_TOKEN` if running locally, set this to a PAT generated in the workspace where the SQL Warehouse is deployed
3. Run the App:
`streamlit run app.py`

### Remote (Databricks Hosted)
1. Edit the `app.yaml` to reflect the catalog & schema you wish to serve in the UI
2. Create a new App in Databricks (New in the top left of workspace)
3. Once container is ready, sync this code to the source code location the app points to in your workspace <br>
`databricks sync --watch . /Workspace/Users/{YOUR_EMAIL}/databricks_apps/{APP_NAME}/`
4. Deploy the app using the UI or with CLI command
`databricks apps deploy {APP_NAME} --source-code-path /Workspace/Users/{YOUR_EMAIL}/databricks_apps/{APP_NAME}/`
## Usage

### Access the App:

When deployed locally, open your browser and navigate to http://localhost:8501 (or the port specified by Streamlit).

When deployed with Databricks, visit the workspace URL given in the App page in your workspace.


### Interact with Tables:

Expand a table to view its data.
Edit existing rows, add new rows, or delete rows as needed.
Click "Save Changes" to apply your modifications.


### Automatic Timestamp Management:

The CreatedAt and UpdatedAt columns are automatically updated and displayed in a human-readable format.


## Architecture

Frontend: Built with Streamlit, providing an interactive and dynamic user interface.
Backend: Connects to Databricks SQL Warehouse using the databricks-sql-connector to execute SQL queries.
Data Handling: Uses pandas DataFrames for data manipulation and temporary views for MERGE operations.

### Key Components

get_connection: Establishes and caches the connection to the Databricks SQL Warehouse.
get_tables: Fetches the list of tables from the specified schema.
get_primary_key: Dynamically retrieves primary keys for each table.
read_table: Fetches the latest data from the table without caching.
save_changes: Handles the MERGE operation, including inserts, updates, and deletes, with automatic timestamp management.
format_value: Formats DataFrame values for SQL insertion.