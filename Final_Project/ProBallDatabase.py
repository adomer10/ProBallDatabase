import os
import pandas as pd
class SimpleDatabase:
    print('Welcome to the ProBall Database')
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

            self.tables.update(batch_tables)

            for table_name in batch_tables:
                self.save_table_to_csv(table_name)

    def insert_data(self, table_name, data, batch_size=1000):
        try:
            if table_name in self.tables:
                df = self.tables[table_name]

                data_lists = [data.split(',') for data in data.splitlines()]

                for i in range(0, len(data_lists), batch_size):
                    batch_data = data_lists[i:i + batch_size]
                    batch_df = pd.DataFrame(batch_data, columns=df.columns)
                    df = pd.concat([df, batch_df], ignore_index=True)

                self.tables[table_name] = df
                self.save_table_to_csv(table_name)
            else:
                print(f"Table '{table_name}' does not exist.")
        except Exception as e:
            print(f"Error inserting data into '{table_name}': {str(e)}")

    def save_table_to_csv(self, table_name):
        try:
            if table_name not in self.tables:
                print(f"Table '{table_name}' does not exist.")
                return

            df = self.tables[table_name]

            temp_file = os.path.join(self.data_directory, f"{table_name}_temp.csv")
            df.to_csv(temp_file, index=False)

            original_file = os.path.join(self.data_directory, f"{table_name}.csv")
            os.remove(original_file)
            os.rename(temp_file, original_file)

        except Exception as e:
            print(f"Error saving data to '{table_name}.csv': {str(e)}")

    def delete_data(self, table_name, conditions, batch_size=1000):
        try:
            if table_name in self.tables:
                df = self.tables[table_name]

                if conditions:
                    condition_mask = df.query(conditions).index

                    for i in range(0, len(condition_mask), batch_size):
                        batch_mask = condition_mask[i:i + batch_size]
                        df = df.drop(batch_mask)

                    self.tables[table_name] = df

                    self.save_table_to_csv(table_name)

                else:
                    print("Conditions for deleting data not provided. No rows deleted.")
            else:
                print(f"Table '{table_name}' does not exist.")
        except Exception as e:
            print(f"Error deleting data from '{table_name}': {str(e)}")

    def sort_merge_order_by(self, df, sort_column, batch_size=1000):

        def merge_sort(data):
            if len(data) <= 1:
                return data

            mid = len(data) // 2
            left = data[:mid]
            right = data[mid:]

            left = merge_sort(left)
            right = merge_sort(right)
            return merge(left, right)

        def merge(left, right):
            result = []
            i, j = 0, 0

            while i < len(left) and j < len(right):
                if left[i][sort_column] <= right[j][sort_column]:
                    result.append(left[i])
                    i += 1
                else:
                    result.append(right[j])
                    j += 1

            result += left[i:]
            result += right[j:]
            return result

        sorted_list = []

        for i in range(0, len(df), batch_size):
            batch = df[i:i + batch_size].to_dict('records')
            sorted_list = merge(sorted_list, merge_sort(batch))

        return pd.DataFrame(sorted_list)
    def get_table_info(self, table_name, conditions, columns, aggregation=None, sort_column=None, batch_size=1000):
        try:
            if table_name in self.tables:
                result = self.tables[table_name]

                if conditions:
                    result = result.query(conditions)

                if columns:
                    result = result[columns]

                if aggregation:
                    group_by_cols = aggregation['group_by']
                    aggregations = aggregation['aggregations']

                    result = self.custom_group_by(result, group_by_cols, aggregations)
                if sort_column:
                    result = self.sort_merge_order_by(result, sort_column, batch_size)

                records = []
                for i in range(0, len(result), batch_size):
                    batch_result = result.iloc[i:i + batch_size]
                    records.extend(batch_result.to_dict('records'))

                return records
            else:
                return f"Table '{table_name}' does not exist."
        except Exception as e:
            print(f"Error processing query: {str(e)}")
            return None

    def custom_group_by(self, df, group_by_cols, aggregations, batch_size=1000):
        try:
            if len(group_by_cols) != 1:
                print("Only one group by column is supported.")
                return df

            group_by_col = group_by_cols[0]

            if group_by_col not in df.columns:
                print(f"Group by column '{group_by_col}' does not exist.")
                return df

            if len(aggregations) != 1:
                print("Only one aggregation is supported.")
                return df

            aggregation = aggregations[0]

            if aggregation.lower() != 'count':
                print(f"Aggregation '{aggregation}' is not supported.")
                return df

            result = pd.DataFrame(columns=[group_by_col, aggregation])

            group_by_index = df.columns.get_loc(group_by_col)

            for i in range(0, len(df), batch_size):
                batch_df = df.iloc[i:i + batch_size]

                group_by_values = batch_df.iloc[:, group_by_index].unique()

                for group_by_value in group_by_values:
                    count = len(batch_df[batch_df[group_by_col] == group_by_value])

                    result = result.append({group_by_col: group_by_value, aggregation: count}, ignore_index=True)

            return result
        except Exception as e:
            print(f"Error performing custom group by: {str(e)}")
            return df

    def create_file(self, table_name, columns):
        try:
            if table_name not in self.tables:
                if columns:
                    df = pd.DataFrame(columns=[col.strip() for col in columns.split(',')])
                else:
                    raise ValueError("Columns must be provided when creating a new table.")

                file_path = os.path.join(self.data_directory, f"{table_name}.csv")
                df.to_csv(file_path, index=False)

                self.tables[table_name] = df

                print(f"File '{table_name}.csv' created successfully.")
            else:
                print(f"Table '{table_name}' already exists. Choose a different name.")
        except Exception as e:
            print(f"Error creating file: {str(e)}")

    def full_outer_join(self, table_names, join_columns, selected_columns=None, conditions=None, batch_size=1000):
        try:
            for table_name in table_names:
                if table_name not in self.tables:
                    print(f"Table '{table_name}' does not exist.")
                    return None

            for table_name in table_names:
                if not all(col in self.tables[table_name].columns for col in join_columns):
                    print(f"Join columns do not exist in '{table_name}'.")
                    return None

            dfs = [self.tables[table_name] for table_name in table_names]

            result = pd.DataFrame()

            for i in range(0, len(dfs), batch_size):
                batch_dfs = dfs[i:i + batch_size]

                batch_result = pd.concat([result] + batch_dfs, axis=1)
                batch_result = batch_result.loc[:, ~batch_result.columns.duplicated()]

                if conditions:
                    condition_mask = batch_result.query(conditions).index
                    batch_result = batch_result.loc[condition_mask]

                if selected_columns:
                    batch_result = batch_result[selected_columns]

                result = pd.concat([result, batch_result], ignore_index=True)

            if conditions:
                condition_mask = result.query(conditions).index
                result = result.loc[condition_mask]

            return result

        except Exception as e:
            print(f"Error performing full outer join: {str(e)}")
            return None
    def display_help(self):
        print("\nAvailable Commands:")
        print("'tables' - See available tables")
        print("'insert' - Insert row into a table")
        print("'delete' - Delete data from a table")
        print("'update' - Update entire row of data in a table")
        print("'query' - Query data from a table")
        print("'join' - Join two tables")
        print("'create' - Create new table")
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
            conditions = input("Enter conditions for deleting "
                               "(e.g., 'player == \"Tyrese Maxey\" and 'season == 2024' or press Enter for all rows): ")
            try:
                db.delete_data(table_name, conditions)
            except ValueError as e:
                print(f"Error deleting data: {str(e)}")
        elif user_input.lower() == 'update':
            table_name = input("Enter the table name: ")
            condition = input("Enter conditions for updating "
                               "(e.g., 'player == \"Tyrese Maxey\" and 'season == 2024' or press Enter for all rows): ")
            update_values = input("Enter values to update (comma-separated): ")
            try:
                db.delete_data(table_name, condition)
                db.insert_data(table_name, update_values)
            except ValueError as e:
                print(f"Error updating data: {str(e)}")
        elif user_input.lower() == 'join':
            table_names = input("Enter the table names to join (comma-separated): ")
            join_columns = input("Enter the columns to join on (comma-separated): ")
            selected_columns = input(
                "Enter the columns to select (comma-separated, or press Enter for all columns): ")
            conditions = input(
                "Enter conditions for joining: ")

            table_names = [table_name.strip() for table_name in table_names.split(',')]
            join_columns = [col.strip() for col in join_columns.split(',')]
            selected_columns = [col.strip() for col in selected_columns.split(',')] if selected_columns else None

            result = None

            if conditions:
                conditions = conditions.strip()

            try:
                result = db.full_outer_join(table_names, join_columns, selected_columns, conditions)
                print("\nJoin Result:")
                print(result)
            except ValueError as e:
                print(f"Error performing join: {str(e)}")
        elif user_input.lower() == 'query':
            table_name = input("Enter the table name: ")
            conditions = input("Enter conditions for querying "
                               "(e.g., 'player == \"Tyrese Maxey\" and 'season == 2024' or press Enter for all rows): ")
            columns_input = input("Enter columns to retrieve (comma-separated, or press Enter for all columns): ")

            group_by_input = input("Enter group by column (or press Enter for no grouping): ")
            aggregations_input = input("Enter count for agg. (press Enter for no aggregations): ")

            columns = [col.strip() for col in columns_input.split(',') if col.strip()] if columns_input else None
            group_by = [col.strip() for col in group_by_input.split(',')] if group_by_input else None
            aggregations = [agg.strip() for agg in aggregations_input.split(',')] if aggregations_input else None

            aggregation = None
            if group_by or aggregations:
                aggregation = {'group_by': group_by, 'aggregations': aggregations}

            result = db.get_table_info(table_name, conditions, columns, aggregation)

            sort_column = input("Enter column to sort by (or press Enter for no sorting): ")
            if sort_column:
                result = db.get_table_info(table_name, conditions, columns, aggregation, sort_column)
            else:
                result = db.get_table_info(table_name, conditions, columns, aggregation)

            print("\nQuery Result:")
            if isinstance(result, list):
                for row in result:
                    print(row)
            else:
                print(result)
        elif user_input.lower() == 'create':
            table_name = input("Enter the table name: ")
            columns = input("Enter column headers (comma-separated, or press Enter for no headers): ")
            db.create_file(table_name, columns)
    else:
                print("Invalid Command")