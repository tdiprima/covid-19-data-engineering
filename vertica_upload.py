"""
Unified data warehouse integration for daily and quarterly uploads
"""

import argparse
import csv
import json
import logging
import os
import subprocess
import time
from datetime import datetime
from pathlib import Path

import sqlalchemy as sa
import sqlalchemy.exc as xx
import vertica_python
from vertica_python.errors import ConnectionError, MissingSchema, QueryError


def get_connection_str(m_str):
    try:
        with open(config_file) as f:
            c = json.load(f)
            connection_str = c[m_str]
    except KeyError as ke:
        logging.error("Wrong key. %s", ke)
        exit(1)
    return connection_str


def connect_postgres(m_str):
    logging.info("Connecting to postgres")
    connect = object()
    try:
        connection_str = get_connection_str(m_str)
        engine = sa.create_engine(connection_str)
        connect = engine.connect()
    except xx.OperationalError as ex:
        logging.error(ex)
        exit(1)

    return connect


def connect_vertica():
    logging.info("Connecting to vertica")
    conn = object()
    with open(config_file) as f:
        conn_info = json.load(f)
        conn_info.pop("pg_str")
        conn_info["use_prepared_statements"] = True
    try:
        conn = vertica_python.connect(**conn_info)
    except ConnectionError as ce:
        logging.error(ce)
        exit(1)

    return conn


def bulk_upload():
    logging.info("Loading data from csv files")
    code_base = Path.cwd()
    my_name = "vertica.sql"
    sql_file_path = code_base / my_name
    if sql_file_path.is_file():
        sql_file_path.unlink()

    csv_files = file_names()
    with open(my_name, "w") as mysql:
        for csv_file in csv_files:
            table_name = csv_file.replace(".csv", "").lower()
            full_table = v_schema + "." + table_name
            file_path = os.path.join(file_location, csv_file)
            insert_str = (
                "COPY "
                + full_table
                + " FROM LOCAL '"
                + file_path
                + "' DELIMITER ',' SKIP 1; \n"
            )
            mysql.write(insert_str)

    try:
        logging.info("Running subprocess")
        return_code = subprocess.Popen("sh batch_load_vertica.sh", shell=True).wait()
        logging.info("return_code: " + str(return_code))
        if return_code > 0:
            logging.error("COPY function didn't work")
            exit(1)
    except OSError as e:
        logging.error(e)
    except ValueError as e:
        logging.error(e)
    except Exception as e:
        logging.error(e)
    finally:
        logging.info("Done loading tables.")


def import_csv2database(file_path, table_name):
    column_list = []
    with open(file_path, encoding="utf-8") as csvfile:
        csv_reader = csv.reader(csvfile, delimiter=",")
        column_count = 0

        for line_count, row in enumerate(csv_reader):
            if line_count == 0:
                column_count = len(row)
                for i in range(column_count):
                    column = row[i]
                    column_list.append(column)
                column_list.append("load_time")
                list_to_str = ",".join(map(str, column_list))
                insert_query1 = (
                    "INSERT INTO "
                    + v_schema
                    + "."
                    + table_name
                    + " ("
                    + list_to_str
                    + ")"
                )
                continue

            value_list = []
            for i in range(column_count):
                column_value0 = row[i]
                column_value = column_value0.replace("'", "''")
                value_list.append(column_value)
            now = datetime.today()
            current_time_str = now.strftime("%Y-%m-%d %H:%M:%S")
            value_list.append(current_time_str)
            value_list_2 = "'%s'" % "','".join(value_list)
            insert_query2 = " VALUES (" + value_list_2 + ")"
            execute_string = insert_query1 + insert_query2

    return execute_string


def insert_tables():
    logging.info("Insert into tables")

    try:
        for num, name in enumerate(file_list):
            path = os.path.join(file_location, name)
            table = name.lower().replace(".csv", "")
            print(path, table)
            insert_str = import_csv2database(path, table)

            try:
                v_cursor.execute(insert_str)
                v_conn.commit()
            except MissingSchema as e:
                logging.error(e)
                exit(1)
            except QueryError as e:
                logging.error(e)
                exit(1)

    except Exception as ex:
        logging.error(ex)
    finally:
        logging.info("Finished loading tables.")


def table_exists(dbcon, tablename):
    if mode == "daily":
        dbcur = dbcon.cursor()
        dbcur.execute(
            f"""
            SELECT COUNT(*)
            FROM v_catalog.tables
            WHERE table_schema = '{v_schema}' AND table_name = '{tablename}'
            """
        )
        return dbcur.fetchone()[0] == 1
    else:
        v_cursor.execute(
            f"""
            SELECT COUNT(*)
            FROM v_catalog.tables
            WHERE table_schema = '{v_schema}' AND table_name = '{tablename}'
            """
        )
        return v_cursor.fetchone()[0] == 1


def copy_table_structure(old_table, new_table):
    x = (
        "CREATE TABLE "
        + new_table
        + " AS (SELECT * FROM "
        + old_table
        + " WHERE 1 = 2);"
    )
    try:
        v_cursor.execute(x)
        v_conn.commit()
    except MissingSchema as e:
        logging.error(e)
        exit(1)
    except QueryError as e:
        logging.error(e)
        exit(1)


def copy2history_table():
    logging.info("Loading history tables")
    for table_name in table_list:
        orig_table = v_schema + "." + table_name
        history_table = v_schema + "." + table_name + "_history"
        has_history_table = table_exists(v_conn, history_table)

        if not has_history_table:
            copy_table_structure(orig_table, history_table)

        execute_str = (
            "INSERT INTO " + history_table + " (SELECT * FROM " + orig_table + ");"
        )
        try:
            v_cursor.execute(execute_str)
            v_conn.commit()
        except MissingSchema as e:
            logging.error(e)
            exit(1)
        except QueryError as e:
            logging.error(e)
            exit(1)


def build_query(table, pg_conn, num):
    if mode == "quarterly":
        print("build_query", table)

    query_pg = (
        "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '"
        + table
        + "' AND table_schema = '"
        + pg_schema
        + "';"
    )

    create_v = ""
    a = "CREATE TABLE IF NOT EXISTS "
    a += v_schema
    a += "."
    a += table
    a += " ("
    b = ""
    c = ");"
    try:
        rows = pg_conn.execute(query_pg).fetchall()
        if len(rows) > 0:
            for row in rows:
                m_name = row[0]
                m_type = row[1]
                if "text" in m_type:
                    m_type = "varchar"
                b += m_name + " " + m_type + ","

            if mode == "quarterly":
                b = b[:-1]
            else:
                b = b[:-1]
        else:
            logging.warning("Postgres table does not exist: " + table)
            if mode == "daily":
                m_files = file_names()
                ff = m_files[num]
            else:
                ff = file_list[num]
            fff = os.path.join(file_location, ff)
            with open(fff) as my_file:
                head = next(my_file).strip().split(",")
            for h in head:
                b += h + " varchar,"

            if mode == "quarterly":
                b += "load_time timestamp"

        create_v = a + b + c

    except Exception as ex:
        logging.error(ex)

    return create_v


def create_tables():
    logging.info("Creating tables")
    pg_conn = connect_postgres("pg_str")

    if mode == "quarterly":
        global v_conn, v_cursor
        v_conn = connect_vertica()
        v_cursor = v_conn.cursor()
        target_list = table_list
    else:
        v_conn = connect_vertica()
        v_cursor = v_conn.cursor()
        target_list = table_names()

    try:
        for num, name in enumerate(target_list):
            if mode == "quarterly":
                print(name)

            if table_exists(v_conn, name):
                v_cursor.execute("DROP TABLE IF EXISTS " + v_schema + "." + name)

            create_str = build_query(name, pg_conn, num)

            try:
                v_cursor.execute(create_str)
                v_conn.commit()
            except MissingSchema as e:
                logging.error(e)
                exit(1)
            except QueryError as e:
                logging.error(e)
                exit(1)

    except Exception as ex:
        logging.error(ex)
    finally:
        if mode == "daily":
            v_conn.close()
        pg_conn.close()
        logging.info("Finished creating tables.")


def file_names():
    m_list = []
    with open("files.list") as f:
        for line in f:
            m_list.append(line.strip())
    return m_list


def table_names():
    m_list = []
    ff = file_names()
    for f in ff:
        m_list.append(f.replace(".csv", "").lower())
    return m_list


def get_lists():
    f_list = []
    t_list = []
    with open("files_quart.list") as f:
        for line in f:
            ll = line.strip()
            f_list.append(ll)
            t_list.append(ll.replace(".csv", "").lower())
    return f_list, t_list


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Vertica upload script")
    parser.add_argument(
        "mode", choices=["daily", "quarterly"], help="Upload mode: daily or quarterly"
    )
    args = parser.parse_args()

    mode = args.mode
    start_time = time.time()
    config_file = "config.json"
    v_schema = "schema_workspace"
    FORMAT = "[%(filename)s:%(lineno)s - %(levelname)s] %(message)s"

    if mode == "daily":
        file_location = "./input"
        pg_schema = "schema_hi"
        log_file = "out_vert_dal.log"
    else:
        file_location = "/data/covid/upload"
        pg_schema = "schema_rtf"
        log_file = "out_vert_quart.log"

    logging.basicConfig(level=logging.INFO, filename=log_file, format=FORMAT)
    logging.info("BEGIN")

    if mode == "daily":
        create_tables()
        bulk_upload()
    else:
        file_list, table_list = get_lists()
        create_tables()
        insert_tables()
        copy2history_table()
        v_conn.close()

    run_time = "--- %s seconds ---" % (time.time() - start_time)
    logging.info("END %s", run_time)
    exit(0)
