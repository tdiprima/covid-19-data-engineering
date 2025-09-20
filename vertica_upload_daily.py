"""
Data warehouse integration
"""

import json
import logging
import os
import subprocess
import time

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


def bulk_upload():
    logging.info("Loading data from csv files")
    code_base = os.getcwd()
    my_name = "vertica.sql"
    sql_file_path = os.path.join(code_base, my_name)
    if os.path.isfile(sql_file_path):  # Delete sql file bc we rewrite it.
        os.remove(sql_file_path)

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
        # run vsql to batch load csv files to database
        logging.info("Running subprocess")
        return_code = subprocess.Popen("sh batch_load_vertica.sh", shell=True).wait()
        logging.info("return_code: " + str(return_code))
        if return_code > 0:
            logging.error("COPY function didn't work")
            exit(1)
    except OSError as e:
        logging.error("{}".format(e))
    except ValueError as e:
        logging.error("{}".format(e))
    except Exception as e:
        logging.error("{}".format(e))
    finally:
        logging.info("Done loading tables.")
    return


def table_exists(dbcon, tablename):
    dbcur = dbcon.cursor()

    dbcur.execute(
        """
        SELECT COUNT(*)
        FROM v_catalog.tables
        WHERE table_schema = '{0}' AND table_name = '{1}'
        """.format(
            v_schema, tablename
        )
    )
    if dbcur.fetchone()[0] == 1:
        return True

    return False


def build_query(table, pg_conn, num):
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
        else:
            logging.warning("Postgres table does not exist: " + table)
            m_files = file_names()
            ff = m_files[num]
            fff = os.path.join(file_location, ff)
            with open(fff) as my_file:
                head = next(my_file).strip().split(",")
            for h in head:
                b += h + " varchar,"

        b = b[:-1]
        create_v = a + b + c

    except Exception as ex:
        logging.error("{}".format(ex))

    return create_v


def create_tables():
    logging.info("Creating tables")
    pg_conn = connect_postgres("pg_str")
    v_conn = connect_vertica()
    v_cursor = v_conn.cursor()
    m_list = table_names()

    try:
        for num, name in enumerate(m_list):

            # If table exists, drop it.
            if table_exists(v_conn, name):
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


if __name__ == "__main__":
    start_time = time.time()
    file_location = "./input"
    config_file = "config.json"
    pg_schema = "schema_hi"
    v_schema = "schema_workspace"  # Testing.
    FORMAT = "[%(filename)s:%(lineno)s - %(levelname)s] %(message)s"
    logging.basicConfig(level=logging.INFO, filename="out_vert_dal.log", format=FORMAT)
    logging.info("BEGIN")
    create_tables()
    bulk_upload()
    run_time = "--- %s seconds ---" % (time.time() - start_time)
    logging.info("END {}".format(run_time))
    exit(0)
