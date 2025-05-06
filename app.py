import os
import streamlit as st
import pandas as pd
from databricks import sql
from databricks.sdk.core import Config
import time

# Initialize Databricks configuration
cfg = Config()

# Ensure warehouse ID is set
assert os.getenv('DATABRICKS_WAREHOUSE_ID'), "DATABRICKS_WAREHOUSE_ID must be set in the environment."
http_path = f"/sql/1.0/warehouses/{os.getenv('DATABRICKS_WAREHOUSE_ID')}"

# Cache the connection
@st.cache_resource
def get_connection():
    user_token = st.context.headers.get('X-Forwarded-Access-Token')
    if not user_token:
        raise ValueError("User token not found in request headers.")
    return sql.connect(
        server_hostname=cfg.host,
        http_path=http_path,
        access_token=user_token
    )

# Fetch list of tables
@st.cache_data
def get_tables(catalog, schema):
    conn = get_connection()
    with conn.cursor() as cursor:
        cursor.execute(f"SHOW TABLES IN {catalog}.{schema}")
        tables = [f"{catalog}.{schema}.{row['tableName']}" for row in cursor.fetchall()]
    return tables

# Dynamically fetch primary keys for a table
@st.cache_data
def get_primary_key(catalog, schema, table):
    conn = get_connection()
    query = f"""
    SELECT kcu.column_name
    FROM {catalog}.information_schema.key_column_usage kcu
    JOIN {catalog}.information_schema.table_constraints tc
    ON kcu.constraint_name = tc.constraint_name
    WHERE tc.constraint_type = 'PRIMARY KEY'
      AND tc.table_schema = '{schema}'
      AND tc.table_name = '{table}'
    """
    with conn.cursor() as cursor:
        cursor.execute(query)
        rows = cursor.fetchall()
        if rows:
            return [row['column_name'] for row in rows]
        return []

# Read table data
def read_table(table_name: str) -> pd.DataFrame:
    conn = get_connection()
    with conn.cursor() as cursor:
        cursor.execute(f"SELECT * FROM {table_name}")
        df = cursor.fetchall_arrow().to_pandas()
    return df

# Save changes with MERGE
def save_changes(table_name: str, source_df: pd.DataFrame, primary_keys: list, all_columns: list, conn):
    if not primary_keys:
        st.error(f"No primary key defined for table {table_name}. Cannot perform upsert or delete.")
        return

    try:
        # Exclude timestamp columns from the temporary view
        columns_to_exclude = ['CreatedAt', 'UpdatedAt']
        df_for_view = source_df.drop(columns=[col for col in columns_to_exclude if col in source_df.columns])
        temp_view = f"temp_{table_name.replace('.', '_')}_{int(time.time())}"
        columns = ', '.join(df_for_view.columns)
        values_list = [
            f"({', '.join([format_value(row[col], df_for_view.dtypes[col]) for col in df_for_view.columns])})"
            for index, row in df_for_view.iterrows()
        ]
        values_clause = ', '.join(values_list)
        create_view_sql = f"""
        CREATE TEMPORARY VIEW {temp_view} AS
        SELECT * FROM (
            VALUES {values_clause}
        ) AS t ({columns})
        """
        with conn.cursor() as cursor:
            cursor.execute(create_view_sql)

        # Define MERGE components
        on_clause = ' AND '.join([f"t.{pk} = s.{pk}" for pk in primary_keys])
        source_columns = [col for col in all_columns if col not in columns_to_exclude]
        update_set = ', '.join([f"t.{col} = s.{col}" for col in source_columns if col not in primary_keys])
        if update_set:
            update_set += ', '
        update_set += 't.UpdatedAt = CURRENT_TIMESTAMP()'
        insert_columns = ', '.join(all_columns)
        insert_values_list = [
            'CURRENT_TIMESTAMP()' if col in columns_to_exclude else f"s.{col}"
            for col in all_columns
        ]
        insert_values = ', '.join(insert_values_list)

        # Construct MERGE statement with delete clause
        merge_sql = f"""
        MERGE INTO {table_name} AS t
        USING {temp_view} AS s
        ON {on_clause}
        WHEN MATCHED AND s.is_delete = TRUE THEN DELETE
        WHEN MATCHED AND s.is_delete = FALSE THEN UPDATE SET {update_set}
        WHEN NOT MATCHED AND s.is_delete = FALSE THEN INSERT ({insert_columns}) VALUES ({insert_values})
        """

        with conn.cursor() as cursor:
            cursor.execute(merge_sql)
        st.success("Changes saved successfully!")
    except Exception as e:
        st.error(f"Error saving changes: {str(e)}")

# Helper function to format values for SQL
def format_value(value, dtype):
    if pd.isnull(value):
        return 'NULL'
    if dtype == 'bool':
        return str(value).upper()
    if dtype == 'object':
        escaped_value = str(value).replace("'", "''")
        return f"'{escaped_value}'"
    elif dtype in ['int64', 'float64']:
        return str(value)
    else:
        escaped_value = str(value).replace("'", "''")
        return f"'{escaped_value}'"

# Streamlit UI
st.title("Configuration Editor")

# Schema details
catalog = "john_armstrong"
schema = "configurations"
tables = get_tables(catalog, schema)

# Initialize refresh counters in session state
if 'refresh_counters' not in st.session_state:
    st.session_state.refresh_counters = {}

# Display tables
for full_table in tables:
    table_name = full_table.split('.')[-1]
    with st.expander(f"📋 {table_name}", expanded=False):
        primary_keys = get_primary_key(catalog, schema, table_name)
        if not primary_keys:
            st.error(f"No primary key found for table {table_name}. Editing is disabled.")
            continue

        if full_table not in st.session_state.refresh_counters:
            st.session_state.refresh_counters[full_table] = 0

        try:
            original_df = read_table(full_table)
            column_config = {
                "CreatedAt": st.column_config.DatetimeColumn(disabled=True),
                "UpdatedAt": st.column_config.DatetimeColumn(disabled=True)
            }
            edited_df = st.data_editor(
                original_df,
                num_rows="dynamic",
                hide_index=True,
                column_config=column_config,
                key=f"editor_{full_table}_{st.session_state.refresh_counters[full_table]}"
            )
            if st.button("Save Changes", key=f"save_{full_table}"):
                # Detect deleted rows
                original_keys = set(original_df[primary_keys].itertuples(index=False, name=None))
                edited_keys = set(edited_df[primary_keys].itertuples(index=False, name=None))
                deleted_keys = original_keys - edited_keys
                delete_rows = original_df[original_df[primary_keys].apply(lambda row: tuple(row), axis=1).isin(deleted_keys)].copy()
                # Set non-primary key columns to None for delete rows
                for col in delete_rows.columns:
                    if col not in primary_keys:
                        delete_rows[col] = None
                # Create source_df with is_delete column
                source_df = pd.concat([edited_df.assign(is_delete=False), delete_rows.assign(is_delete=True)])
                conn = get_connection()
                save_changes(full_table, source_df, primary_keys, list(original_df.columns), conn)
                st.session_state.refresh_counters[full_table] += 1
        except Exception as e:
            st.error(f"Error loading table {table_name}: {str(e)}")

# Sidebar instructions
st.sidebar.title("Instructions")
st.sidebar.write("Expand a table to edit its data. Add, edit, or delete rows, then click 'Save Changes' to upsert or delete data using the table's primary key(s). Timestamps (CreatedAt and UpdatedAt) are automatically set to the current time on save and are not editable.")