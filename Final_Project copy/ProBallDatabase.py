import os
import pandas as pd

class SimpleDatabase:
    def __init__(self, data_directory='.'):
        self.tables = {}
        self.data_directory = data_directory

    def load_all_tables_from_csv(self):
        for file_name in os.listdir(self.data_directory):
            if file_name.endswith('.csv'):
                table_name = os.path.splitext(file_name)[0]
                file_path = os.path.join(self.data_directory, file_name)
                self.load_table_from_csv(table_name, file_path)

    def load_table_from_csv(self, table_name, file_path):
        try:
            df = pd.read_csv(file_path)
            self.tables[table_name] = df
        except FileNotFoundError:
            print(f"Error: File '{file_path}' not found.")

    def insert_data(self, table_name, data):
        try:
            if table_name in self.tables:
                df = self.tables[table_name]
                df = df.append(pd.DataFrame(data), ignore_index=True)
                self.tables[table_name] = df
                self.save_table_to_csv(table_name)
                print(f"Data inserted into '{table_name}' successfully.")
            else:
                print(f"Table '{table_name}' does not exist.")
        except Exception as e:
            print(f"Error inserting data into '{table_name}': {str(e)}")

    def delete_data(self, table_name, conditions):
        try:
            if table_name in self.tables:
                df = self.tables[table_name]
                if conditions:
                    df = df.query(conditions)
                self.tables[table_name] = df
                self.save_table_to_csv(table_name)
                print(f"Data deleted from '{table_name}' successfully.")
            else:
                print(f"Table '{table_name}' does not exist.")
        except Exception as e:
            print(f"Error deleting data from '{table_name}': {str(e)}")

    def update_data(self, table_name, conditions, update_values):
        try:
            if table_name in self.tables:
                df = self.tables[table_name]
                if conditions:
                    df = df.query(conditions)
                if not df.empty:
                    df.update(pd.DataFrame(update_values))
                    self.tables[table_name] = df
                    self.save_table_to_csv(table_name)
                    print(f"Data updated in '{table_name}' successfully.")
                else:
                    print("No matching rows found.")
            else:
                print(f"Table '{table_name}' does not exist.")
        except Exception as e:
            print(f"Error updating data in '{table_name}': {str(e)}")

    def save_table_to_csv(self, table_name):
        file_path = os.path.join(self.data_directory, f"{table_name}.csv")
        self.tables[table_name].to_csv(file_path, index=False)

    def get_table_info(self, table_name, conditions, columns, aggregation=None):
        try:
            if table_name in self.tables:
                result = self.tables[table_name]

                # Apply conditions
                if conditions:
                    result = result.query(conditions)

                # Filter columns if specified
                if columns:
                    result = result[columns]

                # Perform aggregation if specified
                if aggregation:
                    group_by_cols = aggregation['group_by']
                    aggregations = aggregation['aggregations']

                    grouped_result = result.groupby(group_by_cols).agg(aggregations).reset_index()

                    # Optionally fill NaN values
                    grouped_result = grouped_result.fillna(0)  # You can adjust as needed

                    return grouped_result.to_dict('records')
                else:
                    return result.to_dict('records')
            else:
                return f"Table '{table_name}' does not exist."
        except Exception as e:
            print(f"Error processing query: {str(e)}")
            return None

    def display_help(self):
        print("\nAvailable Commands:")
        print("'tables' - See available tables")
        print("'insert' - Insert data into a table")
        print("'delete' - Delete data from a table")
        print("'update' - Update data in a table")
        print("'exit' - Quit the program")
        print("For querying:")
        print("Enter the table name, conditions, columns, and aggregation details as prompted.")

# Example Usage
db = SimpleDatabase()

# Load all tables from CSV files in the current directory
db.load_all_tables_from_csv()

# Command-line interface
while True:
    try:
        user_input = input("\nEnter table name or command ('help' for more help, 'exit' to quit): ")

        if user_input.lower() == 'exit':
            break
        elif user_input.lower() == 'tables':
            print("\nAvailable Tables:", list(db.tables.keys()))
        elif user_input.lower() == 'help':
            db.display_help()
        elif user_input.lower() == 'insert':
            table_name = input("Enter the table name: ")
            data_input = input("Enter data for the table in JSON format: ")
            try:
                data = pd.read_json(data_input).to_dict('records')
                db.insert_data(table_name, data)
            except ValueError as e:
                print(f"Error inserting data: {str(e)}")
        elif user_input.lower() == 'delete':
            table_name = input("Enter the table name: ")
            conditions = input("Enter conditions for deleting data (e.g., 'player == \"Tyrese Maxey\"'): ")
            db.delete_data(table_name, conditions)
        elif user_input.lower() == 'update':
            table_name = input("Enter the table name: ")
            conditions = input("Enter conditions for updating data (e.g., 'player == \"Tyrese Maxey\"'): ")
            update_input = input("Enter data to update in JSON format: ")
            try:
                update_values = pd.read_json(update_input).to_dict('records')[0]
                db.update_data(table_name, conditions, update_values)
            except ValueError as e:
                print(f"Error updating data: {str(e)}")
        else:
            table_name = user_input

            conditions = input("Enter conditions for querying (e.g., 'player == \"Tyrese Maxey\" and 'season == 2024' or press Enter for all rows): ")
            columns_input = input("Enter columns to retrieve (comma-separated, or press Enter for all columns): ")
            aggregation_input = input("Enter aggregation details (e.g., 'group_by: Season, aggregations: mean(Score), sum(Attempts)'; press Enter for no aggregation): ")

            columns = [col.strip() for col in columns_input.split(',') if col.strip()] if columns_input else None
            aggregation = None
            if aggregation_input:
                group_by, aggregations = map(str.strip, aggregation_input.split(';'))
                group_by = [col.strip() for col in group_by.split(',')]
                aggregations = [agg.strip() for agg in aggregations.split(',')]
                aggregation = {'group_by': group_by, 'aggregations': aggregations}

            result = db.get_table_info(table_name, conditions, columns, aggregation)

            print("\nQuery Result:")
            if isinstance(result, list):
                for row in result:
                    print(row)
            else:
                print(result)
    except Exception as e:
        print(f"An error occurred: {str(e)}. Please try again.")
