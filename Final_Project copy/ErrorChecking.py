import pandas as pd

class SimpleDatabase:
    def __init__(self):
        self.tables = {}

    def load_table_from_csv(self, table_name, file_path):
        try:
            df = pd.read_csv(file_path)
            self.tables[table_name] = df
            print(f"Table '{table_name}' loaded successfully from '{file_path}'.")
        except FileNotFoundError:
            print(f"Error: File '{file_path}' not found.")

    def insert_table(self, table_name, data):
        try:
            df = pd.DataFrame(data)
            self.tables[table_name] = df
            print(f"Table '{table_name}' inserted successfully.")
        except Exception as e:
            print(f"Error inserting table '{table_name}': {str(e)}")

    def get_table_info(self, table_name, conditions, columns, aggregation=None):
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

    def display_help(self):
        print("\nAvailable Commands:")
        print("'tables' - See available tables")
        print("'insert' - Insert a new table")
        print("'exit' - Quit the program")
        print("For querying:")
        print("Enter the table name")

# Example Usage
db = SimpleDatabase()

# Command-line interface
while True:
    user_input = input("\nEnter table name or command ('help' for more help with commands, 'exit' to quit): ")

    if user_input.lower() == 'exit':
        break
    elif user_input.lower() == 'tables':
        print("\nAvailable Tables:", list(db.tables.keys()))
    elif user_input.lower() == 'insert':
        table_name = input("Enter the table name: ")
        data_input = input("Enter data for the table: ")
        try:
            data = pd.read_json(data_input).to_dict('records')
            db.insert_table(table_name, data)
        except ValueError as e:
            print(f"Error inserting table: {str(e)}")
    elif user_input.lower() == 'help':
        db.display_help()
    else:
        table_name = user_input

        conditions = input("Enter conditions for querying (e.g., 'Score > 20', or press Enter for all rows): ")
        columns_input = input("Enter columns to retrieve (comma-separated, or press Enter for all columns): ")
        columns = [col.strip() for col in columns_input.split(',') if col.strip()] if columns_input else None

        aggregation_input = input("Enter aggregation details (e.g., 'group_by: Season, aggregations: mean(Score), sum(Attempts)'; press Enter for no aggregation): ")
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
