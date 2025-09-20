"""
Quarterly analytics
"""

import csv
import json
import logging
import os
import time
from datetime import datetime

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
        logging.error("Wrong key. {}".format(ke))
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
        logging.error("{}".format(ex))
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
        logging.error("{}".format(ce))
        exit(1)

    return conn


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

            # Read next row
            value_list = []
            for i in range(column_count):
                column_value0 = row[i]
                # escape single quote
                column_value = column_value0.replace("'", "''")
                value_list.append(column_value)
                # add current date time to database
            now = datetime.today()
            # dd/mm/YY H:M:S
            # current_time_str= now.strftime("%d/%m/%Y %H:%M:%S")
            # format as 2014-07-05 14:34:14
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

            # Update table in vertica
            try:
                v_cursor.execute(insert_str)
                v_conn.commit()
            except MissingSchema as e:
                logging.error("{}".format(e))
                exit(1)
            except QueryError as e:
                logging.error("{}".format(e))
                exit(1)

    except Exception as ex:
        logging.error("{}".format(ex))
    finally:
        logging.info("Finished loading tables.")


def table_exists(tablename):
    v_cursor.execute(
        """
        SELECT COUNT(*)
        FROM v_catalog.tables
        WHERE table_schema = '{0}' AND table_name = '{1}'
        """.format(
            v_schema, tablename
        )
    )
    if v_cursor.fetchone()[0] == 1:
        return True

    return False


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
        logging.error("{}".format(e))
        exit(1)
    except QueryError as e:
        logging.error("{}".format(e))
        exit(1)


def copy2history_table():
    logging.info("Loading history tables")
    for table_name in table_list:
        orig_table = v_schema + "." + table_name
        history_table = v_schema + "." + table_name + "_history"
        has_history_table = table_exists(history_table)

        if not has_history_table:
            copy_table_structure(orig_table, history_table)

        execute_str = (
            "INSERT INTO " + history_table + " (SELECT * FROM " + orig_table + ");"
        )
        try:
            v_cursor.execute(execute_str)
            v_conn.commit()
        except MissingSchema as e:
            logging.error("{}".format(e))
            exit(1)
        except QueryError as e:
            logging.error("{}".format(e))
            exit(1)


def build_query(table, pg_conn, num):
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
            # b += "load_time timestamp"  # Already there.
            b = b[:-1]
        else:
            logging.warning("Postgres table does not exist: " + table)
            ff = file_list[num]
            fff = os.path.join(file_location, ff)
            with open(fff) as my_file:
                head = next(my_file).strip().split(",")
            for h in head:
                b += h + " varchar,"
            b += "load_time timestamp"  # Add it.

        create_v = a + b + c

    except Exception as ex:
        logging.error("{}".format(ex))

    return create_v


def create_tables():
    logging.info("Creating tables")
    pg_conn = connect_postgres("pg_str")

    try:
        for num, name in enumerate(table_list):
            print(name)

            # If table exists, drop it.
            if table_exists(name):
                v_cursor.execute("DROP TABLE IF EXISTS " + v_schema + "." + name)

            create_str = build_query(name, pg_conn, num)

            # Create table in vertica
            try:
                v_cursor.execute(create_str)
                v_conn.commit()
            except MissingSchema as e:
                logging.error("{}".format(e))
                exit(1)
            except QueryError as e:
                logging.error("{}".format(e))
                exit(1)

    except Exception as ex:
        logging.error("{}".format(ex))
    finally:
        pg_conn.close()
        logging.info("Finished creating tables.")


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
    start_time = time.time()
    file_location = "/data/covid/upload"
    config_file = "config.json"
    pg_schema = "schema_rtf"
    v_schema = "schema_workspace"  # Testing.
    FORMAT = "[%(filename)s:%(lineno)s - %(levelname)s] %(message)s"
    logging.basicConfig(
        level=logging.INFO, filename="out_vert_quart.log", format=FORMAT
    )

    logging.info("BEGIN")
    v_conn = connect_vertica()
    v_cursor = v_conn.cursor()
    file_list, table_list = get_lists()
    create_tables()
    insert_tables()
    copy2history_table()
    v_conn.close()
    run_time = "--- %s seconds ---" % (time.time() - start_time)
    logging.info("END {}".format(run_time))
    exit(0)
