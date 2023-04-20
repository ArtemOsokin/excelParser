import abc
from typing import List

import pandas as pd
import datetime as dt


class Excel(abc.ABC):
    def __init__(self, path_to_file='source.xlsx'):
        self.path_to_file = path_to_file

    @abc.abstractmethod
    def extract(self):
        """Извлечение данных"""

    @abc.abstractmethod
    def transform(self):
        """Преобразование извлеченных данных"""


class ProdExcel(Excel):

    def extract(self):
        return pd.read_excel(self.path_to_file, usecols='A:J', header=2)

    def transform(self) -> List[list]:
        df = self.extract()
        data = [list(row) for row in df.values]
        return data

    def transform_with_date(self) -> List[tuple]:
        rows = self.transform()
        yyyy, mm = 2023, 4
        data = list()
        for row in rows:
            if row[0] < 7:
                dd = 1
            elif row[0] < 15:
                dd = 2
            else:
                dd = 3
            row.append(str(dt.date(yyyy, mm, dd)))
            data.append(tuple(row))
        return data
