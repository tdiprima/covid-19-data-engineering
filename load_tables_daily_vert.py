"""
Advanced Vertica processing
Code to load data to Vertica daily.
"""

import csv
import datetime
import json
import logging
import os
import os.path
import shutil
import subprocess

from dateutil import parser as dateParser
from vertica_python import connect
from vertica_python.errors import ConnectionError, MissingSchema, QueryError


def getDBConnection():
    global conn
    with open("config.json") as f:
        conn_info = json.load(f)
        # v_uri = conn_info['v_uri']
        conn_info["use_prepared_statements"] = True
    try:
        conn = connect(**conn_info)
    except ConnectionError as ce:
        msg = f"getDBConnection() : {ce}"
        logging.error(msg)
        print(msg)

    return conn


def createTable(file_path, tbl_name_and_schema):
    column_list = []
    with open(file_path, "r", encoding="utf-8") as csvfile:
        csv_reader = csv.reader(csvfile, delimiter=",")
        for line_count, row in enumerate(csv_reader):
            if line_count == 0:
                column_count = len(row)
                for i in range(column_count):
                    column = row[i]
                    column2 = column + " varchar"
                    column_list.append(column2)
                # new_column="load_time timestamp"
                # column_list.append(new_column)
                listToStr = ",".join(map(str, column_list))
                # drop_query = "DROP TABLE IF EXISTS " + tbl_name_and_schema + ";"
                drop_query = "DROP TABLE IF EXISTS " + tbl_name_and_schema + " CASCADE;"
                create_query = (
                    "CREATE TABLE " + tbl_name_and_schema + " (" + listToStr + ");"
                )
                break

        try:
            # Drop it if it exists
            cur.execute(drop_query)
            conn.commit()
        except Exception as ex:
            print(drop_query)
            print(ex)

        try:
            # If anything fails at this point, exit immediately.
            cur.execute(create_query)
            conn.commit()
        except MissingSchema as e:
            msg = f"createTable() : {e}"
            logging.error(msg)
            print(msg)
            exit(1)
        except QueryError as e:
            msg = f"createTable() : {e}"
            logging.error(msg)
            print(msg)
            exit(1)


def import_csv2database(file_path, table_name):
    column_list = []
    with open(file_path, "r", encoding="utf-8") as csvfile:
        csv_reader = csv.reader(csvfile, delimiter=",")
        column_count = 0

        for line_count, row in enumerate(csv_reader):
            if line_count == 0:
                column_count = len(row)
                for i in range(column_count):
                    column = row[i]
                    column_list.append(column)
                    # column_list.append("load_time")
                listToStr = ",".join(map(str, column_list))
                insert_query1 = "INSERT INTO " + table_name + " (" + listToStr + ")"
                continue

            # Read next row
            value_list = []
            for i in range(column_count):
                column_value0 = row[i]
                # escape single quote
                column_value = column_value0.replace("'", "''")
                value_list.append(column_value)
            # add current date time to database
            # now = datetime.datetime.today()
            # format as 2014-07-05 14:34:14
            # current_time_str= now.strftime("%Y-%m-%d %H:%M:%S")
            # value_list.append(current_time_str)
            value_list_2 = "'%s'" % "','".join(value_list)
            insert_query2 = " VALUES (" + value_list_2 + ")"
            execute_string = insert_query1 + insert_query2
            # print ("-------------- " + str(line_count) + " -----------------------")
            try:
                cur.execute(execute_string)
                conn.commit()
            except QueryError as qe:
                print(line_count, len(row))
                print(execute_string)
                print(f"import_csv2database() : {qe}")
            except Exception as exc:
                print(line_count, len(row))
                print(f"import_csv2database() : {exc}")
            finally:
                print(line_count, len(row))
                print(execute_string)


def backup_history_file(file_location, csv_name, history_folder, date_time_str):
    file_path = os.path.join(file_location, csv_name)
    new_folder = "upload" + "_" + date_time_str
    new_path = os.path.join(history_folder, new_folder)
    if not os.path.exists(new_path):
        os.makedirs(new_path)
    new_csv_file_path = os.path.join(new_path, csv_name)
    shutil.copy2(file_path, new_csv_file_path)


def switch_db_table(table_schemas, table_name):
    full_table_name = table_schemas + "." + table_name
    table_name_build = table_schemas + "." + table_name + "_build"
    return_value = is_table_exist(full_table_name)

    execute_str = ""
    if return_value:
        # execute_str = "DROP TABLE " + full_table_name + "; "
        execute_str = "DROP TABLE " + full_table_name + " CASCADE; "

    execute_str += "ALTER TABLE " + table_name_build + " RENAME TO " + table_name + "; "

    try:
        cur.execute(execute_str)
        conn.commit()
    except QueryError as e:
        print(f"switch_db_table() : {e}")
    except Exception as e:
        print(f"switch_db_table() : {e}")
    finally:
        print(execute_str)


def is_table_exist(table_name):
    select_str = "SELECT EXISTS ("
    select_str += "SELECT * FROM v_catalog.tables "
    select_str += "WHERE table_schema = '"
    select_str += table_schemas + "' "
    select_str += "AND table_name = '"
    select_str += table_name + "') ; "

    try:
        cur.execute(select_str)
        # print (result,result[0][0])
        # table does not exist.
        if cur.rowcount == 0:
            print("No rows to return!")
            return False
        else:
            result = cur.fetchall()
            return result[0][0]  # It's either True or False

    except QueryError as e:
        print(f"is_table_exist() : {e}")
        logging.error("is_table_exist() : %s", e)
        return "error"
    except Exception as e:
        print(f"is_table_exist() : {e}")
        logging.error("is_table_exist() : %s", e)
        return "error"


def getReturnList(query_str):
    item_list = []
    try:
        cur.execute(query_str)
        if cur.rowcount == 0:
            # No rows to return
            return []
        else:
            result = cur.fetchall()
            for item in result:
                item_list.append(item[0])
            return item_list
    except QueryError as e:
        print(f"getReturnList() : {e}")
        return []
    except Exception as e:
        print(f"getReturnList() : {e}")


def getRecordCount(table_schema, table_name):
    table_name2 = table_schema + "." + table_name
    select_string = "SELECT count(*) FROM " + table_name2 + ";"
    try:
        cur.execute(select_string)
        if cur.rowcount == 0:
            # No rows to return
            return 0
        else:
            result = cur.fetchall()
            # print (result)
            record_count = result[0][0]
            return record_count
    except QueryError as e:
        print(f"getRecordCount() : {e}")
        return "error"
    except Exception as e:
        print(f"getRecordCount() : {e}")
        return "error"


def isBool(string):
    return str(string).lower() in ["true", "false", "t", "f"]


def isInteger(string):
    try:
        a = float(string)
        n = int(a)
        return a == n
    except Exception:
        return False


def isNumeric(string):
    try:
        float(string)
        return True
    except Exception:
        return False


def isDate(string):
    try:
        dt = dateParser.parse(string)
        return (dt.hour, dt.minute, dt.second) == (0, 0, 0)
    except Exception:
        return False


def isTimestamp(string):
    try:
        dateParser.parse(string)
        return True
    except Exception:
        return False


def guessType(s):
    # If our field is null, then we have no guess, so return None
    if not s:
        return "varchar"

    if isNumeric(s):
        try:
            if float(s) == int(float(s)):
                if s == "0" or s == "1":
                    return "smallint"

                if str(s)[0] == "0":
                    return "varchar"

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
        if isBool(s):
            return "boolean"

        if isTimestamp(s):
            if isDate(s):
                return "date"
            else:
                return "timestamp"

    return "varchar"


def copyTableStructure(oldTable, newTable):
    execute_str = " SELECT * INTO " + newTable
    execute_str += " FROM " + oldTable
    execute_str += " WHERE 1 = 2;"
    try:
        cur.execute(execute_str)
    except QueryError as e:
        print(f"copyTableStructure() : {e}")
    except Exception as e:
        print(f"copyTableStructure() : {e}")
    finally:
        print(execute_str)


def alterColumn(table_schema, table_name):
    db_table = table_name + "_build"
    record_count = getRecordCount(table_schema, db_table)
    if record_count > 10000:
        limit_count = 1000
    elif record_count > 5000:
        limit_count = 500
    elif record_count > 1000:
        limit_count = 500
    else:
        limit_count = record_count
    print("limit_count", str(limit_count))

    select_query = "SELECT column_name "
    select_query += "FROM v_catalog.columns "
    select_query += "WHERE table_schema = '"
    select_query += table_schema + "' "
    select_query += "AND table_name = '"
    select_query += db_table + "';"
    column_list = getReturnList(select_query)
    print(column_list)
    tmp_table = table_schema + "." + db_table
    for column in column_list:
        column_type_list = []
        final_column_type = ""
        query_str = "SELECT " + column + " FROM " + tmp_table
        query_str += " WHERE " + column + " IS NOT NULL"
        query_str += " LIMIT " + str(limit_count) + "; "
        value_list = getReturnList(query_str)
        for value0 in value_list:
            value = str(value0).strip()
            data_type = guessType(value)
            if data_type == "integer":
                if isTimestamp(value):
                    if isDate(value):
                        data_type = "date"
            column_type_list.append(data_type)

        column_type_list2 = set(column_type_list)
        # print (column_type_list2)
        if len(column_type_list2) == 1:
            final_column_type = list(column_type_list2)[0]
        elif len(column_type_list2) > 1:
            if "varchar" in column_type_list2:
                final_column_type = "varchar"
            elif "timestamp" in column_type_list2:
                final_column_type = "timestamp"
            elif "date" in column_type_list2:
                if "integer" in column_type_list2:
                    final_column_type = "integer"
            elif "numeric" in column_type_list2:
                final_column_type = "numeric"
            elif "bigint" in column_type_list2:
                final_column_type = "bigint"
            elif "integer" in column_type_list2:
                final_column_type = "integer"
            elif "smallint" in column_type_list2:
                final_column_type = "smallint"
            else:
                final_column_type = "varchar"
        else:
            final_column_type = "varchar"

        if final_column_type == "":
            final_column_type = "varchar"

        print(db_table, column, final_column_type)

        if final_column_type != "varchar":
            alter_str = "ALTER TABLE " + tmp_table
            alter_str += " ALTER COLUMN "
            alter_str += column
            alter_str += " SET DATA TYPE "
            alter_str += final_column_type
            alter_str += " ALL PROJECTIONS; "

            try:
                cur.execute(alter_str)
                print("")
            except QueryError as e:
                print("***************************")
                print(f"alterColumn() : {e}")
                print("============================")
                continue
            except Exception as e:
                print(f"alterColumn() : {e}")
                continue
            finally:
                print(alter_str)
                print("")


def backUpCsvFiles(file_list, file_location, history_folder):
    d = datetime.datetime.today()
    date_time_str = d.strftime("%Y_%m_%d")

    for csv_file in file_list:
        print(
            "backup_history_file: "
            + file_location
            + ","
            + csv_file
            + ","
            + history_folder
        )
        logging.info(
            "backup_history_file: "
            + file_location
            + ","
            + csv_file
            + ","
            + history_folder
        )
        backup_history_file(file_location, csv_file, history_folder, date_time_str)


def createEmptyTables(file_list, file_location, table_schemas):
    for csv_file in file_list:
        table_name = csv_file.replace(".csv", "").lower()
        full_table = table_schemas + "." + table_name
        full_table_build = full_table + "_build"
        file_path = os.path.join(file_location, csv_file)
        logging.info("Create empty table " + full_table_build)
        createTable(file_path, full_table_build)


def batchLoadCsv2Tables(file_list, file_location, table_schemas):
    code_base = os.getcwd()
    my_name = "batch_load_vertica.sql"
    sql_file_path = os.path.join(code_base, my_name)
    if os.path.isfile(sql_file_path):  # Delete sql file bc we rewrite it.
        os.remove(sql_file_path)

    with open(my_name, "w") as mysql:
        for csv_file in file_list:
            table_name = csv_file.replace(".csv", "").lower()
            full_table = table_schemas + "." + table_name
            full_table_build = full_table + "_build"
            file_path = os.path.join(file_location, csv_file)
            insert_str = (
                "COPY "
                + full_table_build
                + " FROM LOCAL '"
                + file_path
                + "' DELIMITER ',' SKIP 1; \n"
            )
            logging.info("Batch Load Csv2Table " + full_table_build)
            mysql.write(insert_str)
        mysql.close()

    try:
        # run vsql to batch load csv files to database
        return_code = subprocess.Popen("sh batch_load_vertica.sh", shell=True).wait()
        print(return_code)
        if return_code > 0:
            x = "COPY function didn't work"
            logging.info(x)
            print(x)
            exit(1)
    except OSError as e:
        print(f"batchLoadCsv2Tables() : {e}")
        logging.info("batchLoadCsv2Tables() : " + str(e))
    except ValueError as e:
        print(f"batchLoadCsv2Tables() : {e}")
        logging.info("batchLoadCsv2Tables() : " + str(e))
    except Exception as e:
        print(f"batchLoadCsv2Tables() : {e}")
        logging.info("batchLoadCsv2Tables() : " + str(e))
    finally:
        print("DONE LOADING TABLES")
    return


def alterTablesColumn(file_list, table_schemas):
    for csv_file in file_list:
        table_name = csv_file.replace(".csv", "").lower()
        full_table = table_schemas + "." + table_name
        full_table_build = full_table + "_build"
        msg = "-------------- ALTER " + full_table_build + " -----------------------"
        print(msg)
        logging.info(msg)
        alterColumn(table_schemas, table_name)


def switchTablesName(file_list, table_schemas):
    for csv_file in file_list:
        table_name = csv_file.replace(".csv", "").lower()
        print("switch_db_table: " + table_schemas + ", " + table_name)
        logging.info("switch_db_table: " + table_schemas + ", " + table_name)
        switch_db_table(table_schemas, table_name)


def getTablesRecordCount(file_list, table_schemas):
    for csv_file in file_list:
        table_name = csv_file.replace(".csv", "").lower()
        return_val = getRecordCount(table_schemas, table_name)
        full_table_name = table_schemas + "." + table_name
        print(
            "Record count of table " + full_table_name + " is " + str(return_val) + ""
        )
        logging.info(
            "Record count of table " + full_table_name + " is " + str(return_val) + ""
        )


if __name__ == "__main__":
    file_location = "./input/"
    history_folder = "./history"
    table_schemas = "schema_hi"
    # table_schemas = "schema_workspace"  # Testing.

    if not os.path.exists(file_location):
        print("No such directory:", file_location)
        exit(1)

    if not os.path.exists(history_folder):
        print("No such directory:", history_folder)
        exit(1)

    logging.basicConfig(
        level=logging.INFO,
        filename="output.log",
        format="%(asctime)s :: %(levelname)s :: %(name)s :: Line No %(lineno)d :: %(message)s",
    )

    conn = getDBConnection()
    if conn is None:
        exit(1)

    cur = conn.cursor()

    file_list = []
    with open("files.list") as f:
        for line in f:
            file_list.append(line.strip())

    # backUpCsvFiles(file_list, file_location, history_folder)  # Already done in Bridge's program.
    createEmptyTables(file_list, file_location, table_schemas)
    batchLoadCsv2Tables(file_list, file_location, table_schemas)
    # alterTablesColumn(file_list, table_schemas)  # Temporarily turned off.
    switchTablesName(file_list, table_schemas)
    getTablesRecordCount(file_list, table_schemas)

    try:
        if not conn.closed():
            conn.close()
    except Exception as ex:
        print(f"close conn : {ex}")

    exit(0)
