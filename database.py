import abc
import sqlite3
from typing import List, Tuple


def logger(statement):
    print(f"""
    _________________
        EXECUTING
    {statement}
    _________________
    """)


class Database(abc.ABC):
    def __init__(self, uri='source.db'):
        self.uri = uri

    @abc.abstractmethod
    def connection(self):
        """Соединение с БД """

    @abc.abstractmethod
    def execute(
            self,
            sql: str,
            parameters: tuple = None,
            many=False,
            fetchone=False,
            fetchall=False,
            commit=False
    ):
        """Выполнение SQL запроса"""

    @abc.abstractmethod
    def create_table(self):
        """Создание таблицы"""

    @abc.abstractmethod
    def insert_values(self):
        """Вставка значений в БД"""


class ProdDB(Database):

    @property
    def connection(self):
        return sqlite3.connect(self.uri)

    def execute(
            self,
            sql: str,
            parameters: list = None,
            many=False,
            fetchone=False,
            fetchall=False,
            commit=False
    ):
        if not parameters:
            parameters = tuple()
        with self.connection as connection:
            # connection.set_trace_callback(logger)
            cursor = connection.cursor()
            data = None

            if many:
                cursor.executemany(sql, parameters)
            else:
                cursor.execute(sql, parameters)
            if commit:
                connection.commit()
            if fetchone:
                data = cursor.fetchone()
            if fetchall:
                data = cursor.fetchall()
        return data

    def create_table(self):
        sql = """
        CREATE TABLE IF NOT EXISTS Production(
            id INTEGER,
            company VARCHAR(255) NOT NULL,
            fact_qoil_data1 INTEGER NOT NULL,
            fact_qoil_data2 INTEGER NOT NULL,
            fact_qliq_data1 INTEGER NOT NULL,
            fact_qliq_data2 INTEGER NOT NULL,
            forecast_qoil_data1 INTEGER NOT NULL,
            forecast_qoil_data2 INTEGER NOT NULL,
            forecast_qliq_data1 INTEGER NOT NULL,
            forecast_qliq_data2 INTEGER NOT NULL,
            created_at VARCHAR(255) NOT NULL,
            PRIMARY KEY(id)
        );
        """
        return self.execute(sql)

    def insert_values(self, parameters: List[Tuple]):
        sql = """
        INSERT INTO production VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """
        return self.execute(sql, parameters, commit=True, many=True)

    def count_total_group_by_date(
            self,
            parametrs: List[str] | str
    ) -> List[Tuple]:
        sum_args = ')+SUM('.join(parametrs) \
            if isinstance(parametrs, List) else parametrs
        sql = f"""
        SELECT SUM({sum_args}), created_at FROM production
        GROUP BY created_at;
        """
        return self.execute(sql, fetchall=True)
