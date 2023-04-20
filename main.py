import logging
import sqlite3

from excel import ProdExcel
from database import ProdDB


def start():
    doc = ProdExcel()
    db = ProdDB()

    data = doc.transform_with_date()

    try:
        db.create_table()
        db.insert_values(data)
    except sqlite3.OperationalError as er:
        logging.warning(er)
    except sqlite3.IntegrityError as er:
        logging.warning(f'{er}: Data already inserted')

    total = {
        'fact_qoil': db.count_total_group_by_date(
            ['fact_qoil_data1', 'fact_qoil_data2']
        ),
        'fact_qliq': db.count_total_group_by_date(
            ['fact_qliq_data1', 'fact_qliq_data2']
        ),
    }
    print(total)


if __name__ == '__main__':
    start()
