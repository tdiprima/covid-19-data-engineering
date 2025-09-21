"""
PostgreSQL/Vertica ETL pipeline

Usage:

# For PostgreSQL (default)
python load_tables_daily.py

# For Vertica
DB_TYPE=vertica python load_tables_daily.py
"""

import csv
import datetime
import json
import logging
import os
import os.path
import shutil
import subprocess
import sys
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, List, Optional

import sqlalchemy as sa
from dateutil import parser as dateParser
from sqlalchemy import exc

try:
    from vertica_python import connect as vertica_connect
    from vertica_python.errors import (ConnectionError, MissingSchema,
                                       QueryError)

    VERTICA_AVAILABLE = True
except ImportError:
    VERTICA_AVAILABLE = False


class DatabaseETL(ABC):
    """Abstract base class for database ETL operations"""

    def __init__(self, config_file: str = "config.json"):
        self.config_file = config_file
        self.connection = None
        self.cursor = None
        self.default_data_type = "text"

    @abstractmethod
    def get_db_connection(self):
        """Establish database connection"""

    @abstractmethod
    def execute_query(self, query: str, params: Optional[Any] = None):
        """Execute a database query"""

    @abstractmethod
    def fetch_results(self, query: str) -> List[Any]:
        """Fetch query results"""

    @abstractmethod
    def get_table_exists_query(self, table_name: str, schema: str) -> str:
        """Get query to check if table exists"""

    @abstractmethod
    def get_columns_query(self, table_name: str, schema: str) -> str:
        """Get query to retrieve table columns"""

    @abstractmethod
    def get_alter_column_syntax(
        self, table_name: str, column_name: str, data_type: str
    ) -> str:
        """Get ALTER COLUMN syntax for the database"""

    def close_connection(self):
        """Close database connection"""
        if self.connection:
            try:
                if hasattr(self.connection, "closed") and not self.connection.closed():
                    self.connection.close()
                elif hasattr(self.connection, "close"):
                    self.connection.close()
            except Exception as e:
                print(f"Error closing connection: {e}")

    def is_bool(self, string: str) -> bool:
        return string.lower() in ("true", "false", "t", "f")

    def is_integer(self, string: str) -> bool:
        try:
            a = float(string)
            n = int(a)
            return a == n
        except Exception:
            return False

    def is_numeric(self, string: str) -> bool:
        try:
            float(string)
            return True
        except Exception:
            return False

    def is_date(self, string: str) -> bool:
        try:
            dt = dateParser.parse(string)
            return (dt.hour, dt.minute, dt.second) == (0, 0, 0)
        except Exception:
            return False

    def is_timestamp(self, string: str) -> bool:
        try:
            dateParser.parse(string)
            return True
        except Exception:
            return False

    def guess_type(self, s: str) -> str:
        if not s:
            return self.default_data_type

        if self.is_numeric(s):
            try:
                if float(s) == int(float(s)):
                    if s in ("0", "1"):
                        return "smallint"

                    if s[0] == "0":
                        return self.default_data_type

                    if -32768 <= int(float(s)) <= 32767:
                        return "smallint"

                    if -2147483648 <= int(float(s)) <= 2147483647:
                        return "integer"
                    else:
                        return "bigint"
                else:
                    return "numeric"
            except Exception:
                return "numeric"
        else:
            if self.is_bool(s):
                return "boolean"

            if self.is_timestamp(s):
                if self.is_date(s):
                    return "date"
                else:
                    return "timestamp"

        return self.default_data_type

    def create_table(self, file_path: str, table_name: str):
        """Create table based on CSV structure"""
        column_list = []
        with open(file_path, "r", encoding="utf-8") as csvfile:
            csv_reader = csv.reader(csvfile, delimiter=",")
            for line_count, row in enumerate(csv_reader):
                if line_count == 0:
                    for column in row:
                        column_list.append(f"{column} {self.default_data_type}")
                    break

        columns_str = ",".join(column_list)
        drop_query = f"DROP TABLE IF EXISTS {table_name} CASCADE;"
        create_query = f"CREATE TABLE {table_name} ({columns_str});"

        try:
            self.execute_query(drop_query)
            print(f"Dropped table {table_name}")
        except Exception as e:
            print(f"Drop table error: {e}")

        try:
            self.execute_query(create_query)
            print(f"Created table {table_name}")
        except Exception as e:
            print(f"Create table error: {e}")
            sys.exit(1)

    def import_csv_to_database(self, file_path: str, table_name: str):
        """Import CSV data to database table"""
        with open(file_path, "r", encoding="utf-8") as csvfile:
            csv_reader = csv.reader(csvfile, delimiter=",")

            for line_count, row in enumerate(csv_reader):
                if line_count == 0:
                    columns = ",".join(row)
                    insert_query_base = f"INSERT INTO {table_name} ({columns})"
                    continue

                # Escape single quotes
                escaped_values = [val.replace("'", "''") for val in row]
                values_str = "'" + "','".join(escaped_values) + "'"
                execute_string = f"{insert_query_base} VALUES ({values_str})"

                try:
                    self.execute_query(execute_string)
                    print(f"Inserted row {line_count}, {len(row)} columns")
                except Exception as e:
                    print(f"Insert error at line {line_count}: {e}")
                    print(f"Query: {execute_string}")

    def backup_history_file(
        self, file_location: str, csv_name: str, history_folder: str, date_time_str: str
    ):
        """Backup CSV file to history folder"""
        file_path = os.path.join(file_location, csv_name)
        new_folder = f"upload_{date_time_str}"
        new_path = os.path.join(history_folder, new_folder)
        if not Path(new_path).exists():
            Path(new_path).mkdir(parents=True)
        new_csv_file_path = os.path.join(new_path, csv_name)
        shutil.copy2(file_path, new_csv_file_path)

    def is_table_exist(self, table_name: str, schema: str) -> bool:
        """Check if table exists"""
        query = self.get_table_exists_query(table_name, schema)
        try:
            results = self.fetch_results(query)
            return bool(results and results[0][0])
        except Exception as e:
            print(f"Table exists check error: {e}")
            return False

    def switch_db_table(self, table_schema: str, table_name: str):
        """Switch build table to production table"""
        full_table_name = f"{table_schema}.{table_name}"
        table_name_build = f"{table_schema}.{table_name}_build"

        if self.is_table_exist(table_name, table_schema):
            drop_query = f"DROP TABLE {full_table_name} CASCADE;"
            try:
                self.execute_query(drop_query)
            except Exception as e:
                print(f"Drop table error: {e}")

        rename_query = f"ALTER TABLE {table_name_build} RENAME TO {table_name};"
        try:
            self.execute_query(rename_query)
            print(f"Renamed table {table_name_build} to {table_name}")
        except Exception as e:
            print(f"Rename table error: {e}")

    def get_return_list(self, query: str) -> List[Any]:
        """Execute query and return list of first column values"""
        try:
            results = self.fetch_results(query)
            return [item[0] for item in results]
        except Exception as e:
            print(f"Query execution error: {e}")
            return []

    def get_record_count(self, table_schema: str, table_name: str) -> int:
        """Get record count for table"""
        table_full = f"{table_schema}.{table_name}"
        query = f"SELECT count(*) FROM {table_full};"
        try:
            results = self.fetch_results(query)
            return results[0][0] if results else 0
        except Exception as e:
            print(f"Record count error: {e}")
            return 0

    def alter_column(self, table_schema: str, table_name: str):
        """Alter column types based on data analysis"""
        db_table = f"{table_name}_build"
        record_count = self.get_record_count(table_schema, db_table)

        if record_count > 10000:
            limit_count = 1000
        elif record_count > 5000:
            limit_count = 500
        elif record_count > 1000:
            limit_count = 500
        else:
            limit_count = record_count

        print(f"Analyzing {limit_count} records for type detection")

        columns_query = self.get_columns_query(db_table, table_schema)
        column_list = self.get_return_list(columns_query)

        tmp_table = f"{table_schema}.{db_table}"
        for column in column_list:
            query = f"SELECT {column} FROM {tmp_table} WHERE {column} IS NOT NULL LIMIT {limit_count};"
            value_list = self.get_return_list(query)

            column_types = [self.guess_type(str(value).strip()) for value in value_list]
            unique_types = set(column_types)

            final_type = self._determine_final_type(unique_types)
            print(f"Column {column}: {final_type}")

            if final_type != self.default_data_type:
                alter_query = self.get_alter_column_syntax(
                    tmp_table, column, final_type
                )
                try:
                    self.execute_query(alter_query)
                    print(f"Altered column {column} to {final_type}")
                except Exception as e:
                    print(f"Alter column error: {e}")
                    continue

    def _determine_final_type(self, type_set):
        """Determine final column type from set of detected types"""
        if len(type_set) == 1:
            return list(type_set)[0]
        elif len(type_set) > 1:
            if self.default_data_type in type_set:
                return self.default_data_type
            elif "timestamp" in type_set:
                return "timestamp"
            elif "date" in type_set:
                if "integer" in type_set:
                    return "integer"
            elif "numeric" in type_set:
                return "numeric"
            elif "bigint" in type_set:
                return "bigint"
            elif "integer" in type_set:
                return "integer"
            elif "smallint" in type_set:
                return "smallint"
        return self.default_data_type

    def backup_csv_files(
        self, file_list: List[str], file_location: str, history_folder: str
    ):
        """Backup all CSV files"""
        date_time_str = datetime.datetime.today().strftime("%Y_%m_%d")
        for csv_file in file_list:
            logging.info(f"Backing up: {file_location}/{csv_file} to {history_folder}")
            self.backup_history_file(
                file_location, csv_file, history_folder, date_time_str
            )

    def create_empty_tables(
        self, file_list: List[str], file_location: str, table_schema: str
    ):
        """Create empty tables for all CSV files"""
        for csv_file in file_list:
            table_name = csv_file.replace(".csv", "").lower()
            full_table_build = f"{table_schema}.{table_name}_build"
            file_path = os.path.join(file_location, csv_file)
            logging.info(f"Creating empty table {full_table_build}")
            self.create_table(file_path, full_table_build)

    def alter_tables_column(self, file_list: List[str], table_schema: str):
        """Alter column types for all tables"""
        for csv_file in file_list:
            table_name = csv_file.replace(".csv", "").lower()
            logging.info(f"Altering columns for {table_schema}.{table_name}")
            self.alter_column(table_schema, table_name)

    def switch_tables_name(self, file_list: List[str], table_schema: str):
        """Switch all build tables to production"""
        for csv_file in file_list:
            table_name = csv_file.replace(".csv", "").lower()
            logging.info(f"Switching table {table_schema}.{table_name}")
            self.switch_db_table(table_schema, table_name)

    def get_tables_record_count(self, file_list: List[str], table_schema: str):
        """Get record counts for all tables"""
        for csv_file in file_list:
            table_name = csv_file.replace(".csv", "").lower()
            count = self.get_record_count(table_schema, table_name)
            full_name = f"{table_schema}.{table_name}"
            print(f"Record count of table {full_name} is {count}")
            logging.info(f"Record count of table {full_name} is {count}")


class PostgreSQLETL(DatabaseETL):
    """PostgreSQL implementation of DatabaseETL"""

    def __init__(self, config_file: str = "config.json"):
        super().__init__(config_file)
        self.default_data_type = "text"

    def get_db_connection(self):
        """Establish PostgreSQL connection using SQLAlchemy"""
        with open(self.config_file) as f:
            config = json.load(f)
            connection_uri = config["connection_uri"]
        engine = sa.create_engine(connection_uri)
        self.connection = engine.connect()
        return self.connection

    def execute_query(self, query: str, params: Optional[Any] = None):
        """Execute PostgreSQL query"""
        try:
            if params:
                self.connection.execute(sa.text(query), params)
            else:
                self.connection.execute(sa.text(query))
        except exc.SQLAlchemyError as e:
            raise e

    def fetch_results(self, query: str) -> List[Any]:
        """Fetch PostgreSQL query results"""
        try:
            result = list(self.connection.execute(sa.text(query)))
            return result
        except exc.SQLAlchemyError as e:
            print(f"Query error: {e}")
            return []

    def get_table_exists_query(self, table_name: str, schema: str) -> str:
        """PostgreSQL table existence query"""
        full_name = f"{schema}.{table_name}"
        return f"SELECT to_regclass('{full_name}');"

    def get_columns_query(self, table_name: str, schema: str) -> str:
        """PostgreSQL columns query"""
        return f"""
            SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table_name}'
        """

    def get_alter_column_syntax(
        self, table_name: str, column_name: str, data_type: str
    ) -> str:
        """PostgreSQL ALTER COLUMN syntax"""
        return f"ALTER TABLE {table_name} ALTER COLUMN {column_name} TYPE {data_type} USING {column_name}::{data_type};"


class VerticaETL(DatabaseETL):
    """Vertica implementation of DatabaseETL"""

    def __init__(self, config_file: str = "config.json"):
        super().__init__(config_file)
        self.default_data_type = "varchar"

    def get_db_connection(self):
        """Establish Vertica connection"""
        if not VERTICA_AVAILABLE:
            raise ImportError("vertica_python package not available")

        with open(self.config_file) as f:
            conn_info = json.load(f)
            conn_info["use_prepared_statements"] = True

        try:
            self.connection = vertica_connect(**conn_info)
            self.cursor = self.connection.cursor()
            return self.connection
        except ConnectionError as e:
            print(f"Vertica connection error: {e}")
            raise

    def execute_query(self, query: str, params: Optional[Any] = None):
        """Execute Vertica query"""
        try:
            if params:
                self.cursor.execute(query, params)
            else:
                self.cursor.execute(query)
            self.connection.commit()
        except (QueryError, MissingSchema) as e:
            raise e

    def fetch_results(self, query: str) -> List[Any]:
        """Fetch Vertica query results"""
        try:
            self.cursor.execute(query)
            if self.cursor.rowcount == 0:
                return []
            return self.cursor.fetchall()
        except QueryError as e:
            print(f"Query error: {e}")
            return []

    def get_table_exists_query(self, table_name: str, schema: str) -> str:
        """Vertica table existence query"""
        return f"""
            SELECT EXISTS (
                SELECT * FROM v_catalog.tables 
                WHERE table_schema = '{schema}' AND table_name = '{table_name}'
            );
        """

    def get_columns_query(self, table_name: str, schema: str) -> str:
        """Vertica columns query"""
        return f"""
            SELECT column_name FROM v_catalog.columns 
            WHERE table_schema = '{schema}' AND table_name = '{table_name}';
        """

    def get_alter_column_syntax(
        self, table_name: str, column_name: str, data_type: str
    ) -> str:
        """Vertica ALTER COLUMN syntax"""
        return f"ALTER TABLE {table_name} ALTER COLUMN {column_name} SET DATA TYPE {data_type} ALL PROJECTIONS;"


def create_etl_instance(db_type: str, config_file: str = "config.json") -> DatabaseETL:
    """Factory function to create appropriate ETL instance"""
    if db_type.lower() == "postgresql":
        return PostgreSQLETL(config_file)
    elif db_type.lower() == "vertica":
        return VerticaETL(config_file)
    else:
        raise ValueError(f"Unsupported database type: {db_type}")


def batch_load_csv_to_tables_postgresql(
    file_list: List[str], file_location: str, table_schema: str
):
    """PostgreSQL-specific batch loading using COPY command"""
    sql_file_path = Path.cwd() / "insert.sql"
    if sql_file_path.is_file():
        sql_file_path.unlink()

    with open("insert.sql", "w") as f:
        for csv_file in file_list:
            table_name = csv_file.replace(".csv", "").lower()
            full_table_build = f"{table_schema}.{table_name}_build"
            file_path = os.path.join(file_location, csv_file)
            insert_str = f"\\COPY {full_table_build} FROM '{file_path}' WITH DELIMITER ',' CSV HEADER;\n"
            logging.info(f"Batch Load Csv2Table {full_table_build}")
            f.write(insert_str)

    try:
        return_code = subprocess.Popen("sh psqlCopy.sh", shell=True).wait()
        print(f"PostgreSQL batch load return code: {return_code}")
    except Exception as e:
        print(f"PostgreSQL batch load error: {e}")
        logging.error(f"PostgreSQL batch load error: {e}")


def batch_load_csv_to_tables_vertica(
    file_list: List[str], file_location: str, table_schema: str
):
    """Vertica-specific batch loading using COPY command"""
    sql_file = "batch_load_vertica.sql"
    sql_file_path = Path.cwd() / sql_file
    if sql_file_path.is_file():
        sql_file_path.unlink()

    with open(sql_file, "w") as f:
        for csv_file in file_list:
            table_name = csv_file.replace(".csv", "").lower()
            full_table_build = f"{table_schema}.{table_name}_build"
            file_path = os.path.join(file_location, csv_file)
            insert_str = f"COPY {full_table_build} FROM LOCAL '{file_path}' DELIMITER ',' SKIP 1;\n"
            logging.info(f"Batch Load Csv2Table {full_table_build}")
            f.write(insert_str)

    try:
        return_code = subprocess.Popen("sh batch_load_vertica.sh", shell=True).wait()
        print(f"Vertica batch load return code: {return_code}")
        if return_code > 0:
            logging.error("Vertica COPY function failed")
            sys.exit(1)
    except Exception as e:
        print(f"Vertica batch load error: {e}")
        logging.error(f"Vertica batch load error: {e}")


def main():
    """Main execution function"""
    logging.basicConfig(
        level=logging.INFO,
        filename="output.log",
        format="%(asctime)s :: %(levelname)s :: %(name)s :: Line No %(lineno)d :: %(message)s",
    )

    # Configuration
    db_type = os.getenv("DB_TYPE", "postgresql")  # Default to PostgreSQL
    file_location = "./input/"
    history_folder = "./history"
    table_schema = "schema_hi"

    # Validate directories
    if not Path(file_location).exists():
        print(f"No such directory: {file_location}")
        sys.exit(1)

    if not Path(history_folder).exists():
        print(f"No such directory: {history_folder}")
        sys.exit(1)

    # Create ETL instance
    try:
        etl = create_etl_instance(db_type)
        etl.get_db_connection()
    except Exception as e:
        print(f"Failed to create ETL instance: {e}")
        sys.exit(1)

    # Get file list
    if db_type.lower() == "vertica" and Path("files.list").exists():
        # Vertica version reads from files.list
        file_list = []
        with open("files.list") as f:
            for line in f:
                file_list.append(line.strip())
    else:
        # PostgreSQL version uses hardcoded list
        file_list = [
            "PH_D_Person_Race.csv",
            "PH_D_Person.csv",
            "PH_F_Claim.csv",
            "PH_D_Person_Alias.csv",
            "PH_D_Person_Demographics.csv",
            "PH_F_Encounter.csv",
            "PH_F_Encounter_Benefit_Coverage.csv",
            "PH_F_Encounter_Location.csv",
            "PH_F_Medication.csv",
            "PH_F_Procedure.csv",
            "PH_F_Condition.csv",
            "PH_F_Result.csv",
            "EMPI_ID_Observation_Period.csv",
            "Map_Between_Claim_Id_Encounter_Id.csv",
            "recent_documents_titles.csv",
            "recent_enc_with_documents.csv",
            "recent_rad_documents_titles.csv",
            "pui_mapped_mrns_to_empi_id.csv",
            "map2_condition_occurrence_with_ccs.csv",
            "hi_care_site.csv",
            "med_admin.csv",
            "med_admin_ingred.csv",
        ]

    try:
        # Execute ETL pipeline
        etl.backup_csv_files(file_list, file_location, history_folder)
        etl.create_empty_tables(file_list, file_location, table_schema)

        # Database-specific batch loading
        if db_type.lower() == "postgresql":
            batch_load_csv_to_tables_postgresql(file_list, file_location, table_schema)
        elif db_type.lower() == "vertica":
            batch_load_csv_to_tables_vertica(file_list, file_location, table_schema)

        # Common operations
        etl.alter_tables_column(file_list, table_schema)
        etl.switch_tables_name(file_list, table_schema)
        etl.get_tables_record_count(file_list, table_schema)

    finally:
        etl.close_connection()

    print("ETL pipeline completed successfully!")


if __name__ == "__main__":
    main()
