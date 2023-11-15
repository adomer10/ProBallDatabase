from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType

class SimpleDatabase:
    def __init__(self, data_directory='.', spark_master='local'):
        self.data_directory = data_directory
        self.spark = SparkSession.builder.master(spark_master).appName("SimpleDatabase").getOrCreate()
        self.tables = {}

    def load_all_tables_from_csv(self):
        for file_name in os.listdir(self.data_directory):
            if file_name.endswith('.csv'):
                table_name = os.path.splitext(file_name)[0]
                file_path = os.path.join(self.data_directory, file_name)
                self.load_table_from_csv(table_name, file_path)

    def load_table_from_csv(self, table_name, file_path):
        try:
            schema = self.infer_schema(file_path)
            df = self.spark.read.csv(file_path, header=True, schema=schema)
            self.tables[table_name] = df
        except FileNotFoundError:
            print(f"Error: File '{file_path}' not found.")

    def infer_schema(self, file_path):
        # This is a simple example. You may need to enhance it based on your actual data.
        sample_df = self.spark.read.csv(file_path, header=True, inferSchema=True, mode="DROPMALFORMED")
        return sample_df.schema

    def insert_data(self, table_name, data):
        try:
            if table_name in self.tables:
                df = self.tables[table_name]
                new_data_df = self.spark.createDataFrame(pd.DataFrame(data), schema=df.schema)
                self.tables[table_name] = df.union(new_data_df)
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
                    df = df.filter(conditions)
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
                    df = df.filter(conditions)
                if not df.isEmpty():
                    for col_name, col_value in update_values.items():
                        df = df.withColumn(col_name, col(lit(col_value)))
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
        self.tables[table_name].write.csv(file_path, header=True, mode='overwrite')

    def get_table_info(self, table_name, conditions, columns, aggregation=None):
        try:
            if table_name in self.tables:
                result = self.tables[table_name]

                # Apply conditions
                if conditions:
                    result = result.filter(conditions)

                # Filter columns if specified
                if columns:
                    result = result.select(columns)

                # Perform aggregation if specified
                if aggregation:
                    group_by_cols = aggregation['group_by']
                    aggregations = aggregation['aggregations']

                    grouped_result = result.groupBy(group_by_cols).agg(aggregations)

                    return grouped_result.collect()
                else:
                    return result.collect()
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
        print("Enter the table name, conditions,
