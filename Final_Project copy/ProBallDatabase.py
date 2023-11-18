import os
import pandas as pd

class SimpleDatabase:
    def __init__(self, data_directory='.'):
        self.tables = {}
        self.data_directory = data_directory

    def load_all_tables_from_csv_in_batches(self, batch_size=1000):
        files = [file_name for file_name in os.listdir(self.data_directory) if file_name.endswith('.csv')]

        for i in range(0, len(files), batch_size):
            batch_files = files[i:i + batch_size]
            batch_tables = {}

            for file_name in batch_files:
                table_name = os.path.splitext(file_name)[0]
                file_path = os.path.join(self.data_directory, file_name)
                batch_tables[table_name] = pd.read_csv(file_path)

            # Update tables in memory
            self.tables.update(batch_tables)

            # Save tables to disk after processing each batch
            for table_name in batch_tables:
                self.save_table_to_csv(table_name)

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

                # Convert data to a list (assuming data is a comma-separated string)
                data_list = data.split(',')

                # Append the new row to the DataFrame
                df.loc[len(df)] = data_list

                self.tables[table_name] = df
                self.save_table_to_csv(table_name)
                print(f"Data inserted into '{table_name}' successfully.")
            else:
                print(f"Table '{table_name}' does not exist.")
        except Exception as e:
            print(f"Error inserting data into '{table_name}': {str(e)}")

    def save_table_to_csv(self, table_name):
        try:
            # Check if the table exists
            if table_name not in self.tables:
                print(f"Table '{table_name}' does not exist.")
                return

            # Get the DataFrame
            df = self.tables[table_name]

            # Save the DataFrame to a temporary CSV file using os
            temp_file = os.path.join(self.data_directory, f"{table_name}_temp.csv")
            df.to_csv(temp_file, index=False)

            # Remove the original file and rename the temporary file using os
            original_file = os.path.join(self.data_directory, f"{table_name}.csv")
            os.remove(original_file)
            os.rename(temp_file, original_file)

            print(f"Data saved to '{table_name}.csv' successfully.")
        except Exception as e:
            print(f"Error saving data to '{table_name}.csv': {str(e)}")

    def update_data(self, table_name, conditions, update_values):
        try:
            if table_name in self.tables:
                df = self.tables[table_name]

                # Apply conditions
                if conditions:
                    df = df.query(conditions)

                if not df.empty:
                    # Convert update_values to a dictionary
                    update_dict = dict(zip(df.columns, map(str.strip, update_values.split(','))))

                    # Update the DataFrame
                    df = df.assign(**update_dict)

                    # Update the original DataFrame with the changes
                    self.tables[table_name].update(df)

                    # Save the updated DataFrame to CSV
                    self.save_table_to_csv(table_name)

                    print(f"Data updated in '{table_name}' successfully.")
                else:
                    print("No matching rows found.")
            else:
                print(f"Table '{table_name}' does not exist.")
        except Exception as e:
            print(f"Error updating data in '{table_name}': {str(e)}")

    def delete_data(self, table_name, conditions):
        try:
            if table_name in self.tables:
                df = self.tables[table_name]

                # Apply conditions
                if conditions:
                    df = df.query(conditions)

                if not df.empty:
                    # Remove the matching rows from the DataFrame
                    df = df[~df.index.isin(df.query(conditions).index)]

                    # Save the updated DataFrame to a CSV file using os
                    temp_file = os.path.join(self.data_directory, f"{table_name}_temp.csv")
                    df.to_csv(temp_file, index=False)

                    # Remove the original file and rename the temporary file using os
                    original_file = os.path.join(self.data_directory, f"{table_name}.csv")
                    os.remove(original_file)
                    os.rename(temp_file, original_file)

                    print(f"Data deleted from '{table_name}' successfully.")
                else:
                    print("No matching rows found.")
            else:
                print(f"Table '{table_name}' does not exist.")
        except Exception as e:
            print(f"Error deleting data from '{table_name}': {str(e)}")

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

    def create_file(self, table_name, data=None):
        try:
            # Check if the table already exists
            if table_name not in self.tables:
                # Create an empty DataFrame
                df = pd.DataFrame()

                # Save the empty DataFrame to a CSV file
                file_path = os.path.join(self.data_directory, f"{table_name}.csv")
                df.to_csv(file_path, index=False)

                # Update tables in memory
                self.tables[table_name] = df

                print(f"File '{table_name}.csv' created successfully.")
            else:
                print(f"Table '{table_name}' already exists. Choose a different name.")
        except Exception as e:
            print(f"Error creating file: {str(e)}")

    def display_help(self):
        print("\nAvailable Commands:")
        print("'tables' - See available tables")
        print("'insert' - Insert data into a table")
        print("'delete' - Delete data from a table")
        print("'update' - Update entire row of data in a table")
        print("'query' - Query data from a table")
        print("'create' - Create a new file")
        print("'exit' - Quit the program")

db = SimpleDatabase()

db.load_all_tables_from_csv_in_batches()

while True:
    while True:
        user_input = input("\nEnter command ('help' for more help, 'exit' to quit): ")

        if user_input.lower() == 'exit':
            break
        elif user_input.lower() == 'tables':
            print("\nAvailable Tables:")
            for key in db.tables.keys():
                print(key)
        elif user_input.lower() == 'help':
            db.display_help()
        elif user_input.lower() == 'insert':
            table_name = input("Enter the table name: ")
            data_input = input("Enter data for the table (comma-separated): ")
            try:
                db.insert_data(table_name, data_input)
            except ValueError as e:
                print(f"Error appending data: {str(e)}")
        elif user_input.lower() == 'delete':
            table_name = input("Enter the table name: ")
            conditions = input("Enter conditions for deleting data (e.g., 'player == \"Tyrese Maxey\"'): ")
            db.delete_data(table_name, conditions)
        elif user_input.lower() == 'update':
            table_name = input("Enter the table name: ")
            conditions = input("Enter conditions for updating data (e.g., 'player == \"Tyrese Maxey\"'): ")
            update_input = input("Enter data to update (comma-separated): ")
            try:
                db.update_data(table_name, conditions, update_input)
            except ValueError as e:
                print(f"Error updating data: {str(e)}")
        elif user_input.lower() == 'query':
            table_name = input("Enter the table name: ")
            conditions = input("Enter conditions for querying "
                               "(e.g., 'player == \"Tyrese Maxey\" and 'season == 2024' or press Enter for all rows): ")
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
        elif user_input.lower() == 'create':
            table_name = input("Enter the table name for the new file: ")
            db.create_file(table_name)
        else:
                print("Invalid Command")
