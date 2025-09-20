"""
PostgreSQL ETL pipeline
"""

import sys
import csv
import datetime
import json
import logging
import os
import os.path
import shutil
import subprocess

import sqlalchemy as sa
from dateutil import parser as dateParser
from sqlalchemy import exc


def getDBConnection():
    with open("config.json") as f:
        config = json.load(f)
        connection_uri = config["connection_uri"]
    engine = sa.create_engine(connection_uri)
    connect = engine.connect()
    return connect


def createTable(file_path, table_name):
    column_list = []
    with open(file_path, "r", encoding="utf-8") as csvfile:
        csv_reader = csv.reader(csvfile, delimiter=",")
        line_count = 0
        for line_count, row in enumerate(csv_reader):
            if line_count == 0:
                column_count = len(row)
                for i in range(column_count):
                    column = row[i]
                    column2 = column + " text"
                    column_list.append(column2)
                # new_column="load_time timestamp"
                # column_list.append(new_column);
                listToStr = ",".join(map(str, column_list))
                create_query = "CREATE TABLE " + table_name + " (" + listToStr + ")"
                break

        print(create_query)
        try:
            connection.execute(create_query)
        except exc.SQLAlchemyError as e:
            print(e)


def import_csv2database(file_path, table_name):
    column_list = []
    with open(file_path, "r", encoding="utf-8") as csvfile:
        csv_reader = csv.reader(csvfile, delimiter=",")
        line_count = 0
        column_count = 0

        for line_count, row in enumerate(csv_reader):
            if line_count == 0:
                column_count = len(row)
                for i in range(column_count):
                    column = row[i]
                    column_list.append(column)
                # column_list.append("load_time");
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
            # add currrent date time to database
            # now = datetime.datetime.today()      #
            # format as 2014-07-05 14:34:14
            # current_time_str= now.strftime("%Y-%m-%d %H:%M:%S")
            # value_list.append(current_time_str);
            value_list_2 = "'%s'" % "','".join(value_list)
            insert_query2 = " VALUES (" + value_list_2 + ")"
            execute_string = insert_query1 + insert_query2
            # print ("--------------  " + str(line_count) +"  -----------------------");
            try:
                connection.execute(sa.text(execute_string))
            except exc.SQLAlchemyError as e:
                print(line_count, len(row))
                print(execute_string)
                print(e)
            except Exception:
                print(line_count, len(row))
                print("Unexpected error:", sys.exc_info()[0])
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

    if return_value:
        execute_str = " BEGIN;"
        execute_str += " DROP TABLE " + full_table_name + ";"
        execute_str += (
            " ALTER TABLE " + table_name_build + " RENAME TO " + table_name + ";"
        )
        execute_str += " COMMIT;"
    else:
        execute_str = " BEGIN;"
        execute_str += (
            " ALTER TABLE " + table_name_build + " RENAME TO " + table_name + ";"
        )
        execute_str += " COMMIT;"

    try:
        connection.execute(sa.text(execute_str))
    except exc.SQLAlchemyError as e:
        print(e)
    except Exception as e:
        print(e)
    finally:
        print(execute_str)


def is_table_exist(table_name):
    select_str = "SELECT to_regclass('" + table_name + "');"
    try:
        result = list(connection.execute(select_str))
        # print (result,result[0][0]);
        # table does not exist.
        if result[0][0] is None:
            return False
        else:
            return True
    except exc.SQLAlchemyError as e:
        print(e)
        logging.error("Exception is : {}".format(e))
        return "error"
    except Exception as e:
        print(e)
        logging.error("Exception is : {}".format(e))
        return "error"


def getReturnList(query_str):
    item_list = []
    try:
        result = list(connection.execute(sa.text(query_str)))
        for item in result:
            item_list.append(item[0])
        return item_list
    except exc.SQLAlchemyError as e:
        print(e)
        return item_list
    except Exception as e:
        print(e)
    finally:
        return item_list


def getRecordCount(table_schema, table_name):
    table_name2 = table_schema + "." + table_name
    select_string = "SELECT count(*) FROM " + table_name2 + ";"
    try:
        result = list(connection.execute(sa.text(select_string)))
        # print (result);
        record_count = result[0][0]
        return record_count
    except exc.SQLAlchemyError as e:
        print(e)
        return "error"
    except Exception as e:
        print(e)
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
    except:
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
        return "text"

    if isNumeric(s):
        try:
            if float(s) == int(float(s)):
                if s == "0" or s == "1":
                    return "smallint"

                if str(s)[0] == "0":
                    return "text"

                if -32768 <= int(float(s)) <= 32767:
                    return "smallint"

                if -2147483648 <= int(float(s)) <= 2147483647:
                    return "integer"
                else:
                    return "bigint"
            else:
                return "numeric"
        except Exception as e:
            print(e)
            return "numeric"
    else:
        if isBool(s):
            return "boolean"

        if isTimestamp(s):
            if isDate(s):
                return "date"
            else:
                return "timestamp"

    return "text"


def copyTableStructure(oldTable, newTable):
    execure_str = " Select * into " + newTable
    execure_str += " from " + oldTable
    execure_str += " Where 1 = 2;"
    try:
        connection.execute(sa.text(execure_str))
    except exc.SQLAlchemyError as e:
        print(e)
    except Exception as e:
        print(e)
    finally:
        print(execure_str)


def alterColumn(table_schema, table_name):
    db_table = table_name + "_" + "build"
    column_list = []
    record_count = getRecordCount(table_schema, db_table)
    limit_count = 1
    if record_count > 10000:
        limit_count = 1000
    elif record_count > 5000:
        limit_count = 500
    elif record_count > 1000:
        limit_count = 500
    else:
        limit_count = record_count
    print(limit_count)

    select_query = (
        "SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '"
    )
    select_query += table_schema + "'"
    select_query += " and TABLE_NAME = '"
    select_query += db_table + "'"
    column_list = getReturnList(select_query)
    print(column_list)
    tmp_table = table_schema + "." + db_table
    for column in column_list:
        column_type_list = []
        final_column_type = ""
        query_str = "SELECT " + column + " FROM " + tmp_table
        query_str += " where " + column + " is not null"
        query_str += " limit " + str(limit_count) + " ;"
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
        # print (column_type_list2);
        if len(column_type_list2) == 1:
            final_column_type = list(column_type_list2)[0]
        elif len(column_type_list2) > 1:
            if "text" in column_type_list2:
                final_column_type = "text"
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
                final_column_type = "text"
        else:
            final_column_type = "text"

        if final_column_type == "":
            final_column_type = "text"

        print(db_table, column, final_column_type)

        if final_column_type != "text":
            alter_str = "ALTER TABLE " + tmp_table
            alter_str += " ALTER COLUMN " + column + " TYPE " + final_column_type
            alter_str += " USING " + column + "::" + final_column_type + ";"
            try:
                connection.execute(sa.text(alter_str))
                print("")
            except exc.SQLAlchemyError as e:
                print("***************************")
                print(e)
                print("============================")
                continue
            except Exception as e:
                print(e)
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
        logging.info("Create empty table  " + full_table_build)
        createTable(file_path, full_table_build)


def batchLoadCsv2Tables(file_list, file_location, table_schemas):
    code_base = os.getcwd()
    sql_file_path = os.path.join(code_base, "insert.sql")
    if os.path.isfile(sql_file_path):  # if insert.sql available, delete it
        os.remove(sql_file_path)

    with open("insert.sql", "w") as mysql:
        for csv_file in file_list:
            table_name = csv_file.replace(".csv", "").lower()
            full_table = table_schemas + "." + table_name
            table_name + "_build"
            full_table_build = full_table + "_build"
            file_path = os.path.join(file_location, csv_file)
            insert_str = (
                "\COPY "
                + full_table_build
                + " FROM '"
                + file_path
                + "' WITH DELIMITER ',' CSV HEADER;\n"
            )
            print(insert_str)
            logging.info("Batch Load Csv2Table  " + full_table_build)
            mysql.write(insert_str)
        mysql.close()

    # run psql to load batch load csv files to database
    try:
        return_code = subprocess.Popen("sh psqlCopy.sh", shell=True).wait()
        print(return_code)
        return
    except OSError as e:
        print(e)
        logging.info("Error occurs in func batchLoadCsv2Tables: " + str(e))
        return
    except ValueError as e:
        print(e)
        logging.info("Error occurs in func batchLoadCsv2Tables: " + str(e))
        return
    except Exception as e:
        print(e)
        logging.info("Error occurs in func batchLoadCsv2Tables: " + str(e))
        return


def alterTablesColumn(file_list, table_schemas):
    for csv_file in file_list:
        table_name = csv_file.replace(".csv", "").lower()
        full_table = table_schemas + "." + table_name
        full_table_build = full_table + "_build"
        print("alterColumn: " + table_schemas + ", " + full_table_build)
        logging.info("alterColumn: " + table_schemas + ", " + full_table_build)
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
            "Record count of table  " + full_table_name + " is " + str(return_val) + ";"
        )
        logging.info(
            "Record count of table  " + full_table_name + " is " + str(return_val) + ";"
        )


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        filename="output.log",
        format="%(asctime)s :: %(levelname)s :: %(name)s :: Line No %(lineno)d :: %(message)s",
    )

    connection = getDBConnection()

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

    file_location = "./input/"
    history_folder = "/data/covid/uploads_timestamped/ph_csv"
    table_schemas = "schema_hi"

    backUpCsvFiles(file_list, file_location, history_folder)
    createEmptyTables(file_list, file_location, table_schemas)
    batchLoadCsv2Tables(file_list, file_location, table_schemas)
    alterTablesColumn(file_list, table_schemas)
    switchTablesName(file_list, table_schemas)
    getTablesRecordCount(file_list, table_schemas)

    connection.close()
    exit()
