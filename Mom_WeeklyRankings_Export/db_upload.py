import psycopg2
import yaml
import pandas as pd
from io import StringIO

from Mom_WeeklyRankings_Export.cust_logging import log_print


class DatabaseCursor(object):
    def __init__(self, credential_file):
        """
        Import database credentials

        credential_file = path to private yaml file
        kwargs = {option_schema: "raw"}
        """
        try:
            with open(credential_file) as file:
                self.credentials = yaml.load(file, Loader=yaml.SafeLoader)

            self.db_url = self.credentials["heroku_db_url"]

        except Exception as e:
            log_print(
                error=e,
                module_="db_psql_model.py",
                func="__init__",
                cred_file="Credential File",
            )

    def __enter__(self):
        """
        Set up connection and cursor
        """

        try:
            self.conn = psycopg2.connect(
                self.db_url,
                sslmode="require",
            )
            self.cur = self.conn.cursor()

            return self.cur

        except (Exception, psycopg2.OperationalError) as e:
            log_print(
                error=e,
                module_="db_psql_model.py",
                func="__enter__",
                connection="Connection Error",
            )

    def __exit__(self, exc_result):
        """
        Close connection and cursor

        exc_results = bool
        """

        if exc_result == True:
            self.conn.commit()

        self.cur.close()
        self.conn.close()

    def copy_to_psql(self, df, table):
        """
        Copy table to postgres from a pandas dataframe
        in memory using StringIO
        https://naysan.ca/2020/05/09/pandas-to-postgresql-using-psycopg2-bulk-insert-performance-benchmark/
        https://stackoverflow.com/questions/23103962/how-to-write-dataframe-to-postgres-table

        table = "test"
        df = pd.DataFrame()
        first_time = "NO"
        """

        buffer = StringIO()
        df.to_csv(buffer, index=False)
        buffer.seek(0)

        try:
            cursor = self.__enter__()
            copy_to = f"BEGIN; \
    DELETE FROM {table}; \
    COPY {table} FROM STDIN WITH (FORMAT CSV, HEADER TRUE); \
END;"
            cursor.copy_expert(copy_to, buffer)
            self.__exit__(exc_result=True)
            log_print(
                success="COPY EXPERT to MenOfMadison",
                module_="db_psql_model.py",
                func="copy_table_to_postgres_new",
                table=table,
                query=copy_to,
            )

        except (Exception, psycopg2.DatabaseError) as e:
            self.__exit__(exc_result=False)
            log_print(
                error=e,
                module_="db_psql_model.py",
                func="copy_to_psql",
                table=table,
                copy_query=copy_to,
            )

    def copy_from_psql(self, query):
        """
        Copy data from Postgresql Query into
        Pandas dataframe
        https://towardsdatascience.com/optimizing-pandas-read-sql-for-postgres-f31cd7f707ab

        query = "select * from raw.test"
        """
        cursor = self.__enter__()

        sql_query = f"COPY ({query}) TO STDOUT WITH (FORMAT CSV, HEADER TRUE);"
        buffer = StringIO()

        try:
            cursor.copy_expert(sql_query, buffer)
            buffer.seek(0)
            df = pd.read_csv(buffer)
            self.__exit__(exc_result=True)
            log_print(
                success="COPY QUERY FROM MenOfMadison",
                module_="db_psql_model.py",
                func="copy_from_psql",
                query=sql_query,
            )
            return df

        except (Exception, psycopg2.DatabaseError) as e:
            self.__exit__(exc_result=False)
            log_print(
                error=e,
                module_="db_psql_model.py",
                func="copy_from_psql",
                query=sql_query,
            )
