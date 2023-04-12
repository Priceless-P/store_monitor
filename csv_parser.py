import csv

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker

from database import StoreStatus, StoreHours, StoreTimeZone, database_engine

Session = sessionmaker(bind=database_engine)
session = Session()


def timezone_parser():
    try:
        with open('store timezone.csv', 'r') as data:
            reader = csv.DictReader(data)
            store_timezone_list = []
            for row in reader:
                store_timezone = {
                    'store_id': row['store_id'],
                    'timezone': row['timezone_str'] or ' America/Chicago'
                }
                store_timezone_list.append(store_timezone)
            stmt = insert(StoreTimeZone).values(store_timezone_list).on_conflict_do_nothing()
            session.execute(stmt)
            session.commit()
    except IntegrityError as e:
        session.rollback()
        print(f"Error: {e}")
    except FileNotFoundError:
        print("File not found: store timezone.csv")


def store_hours_parser():
    try:
        with open('store hours.csv', 'r') as data:
            reader = csv.DictReader(data)
            store_hours_list = []
            for row in reader:
                store_hours = {
                    'store_id': row['store_id'],
                    'day_of_week': row['day'],
                    'start_time_local': row['start_time_local'],
                    'end_time_local': row['end_time_local']
                }
                store_hours_list.append(store_hours)

            stmt = insert(StoreHours).values(store_hours_list).on_conflict_do_nothing()
            session.execute(stmt)
            session.commit()

    except FileNotFoundError:
        print("File not found: store hours.csv")
    except Exception as e:
        session.rollback()
        print(f"Error: {e}")


def store_status_parser():
    try:
        with open('active_aa.csv', 'r') as data:
            reader = csv.DictReader(data)
            store_status_list = []
            for row in reader:
                store_status = {
                    'store_id': row['store_id'],
                    'status': row['status'],
                    'timestamp_utc': row['timestamp_utc']
                }
                store_status_list.append(store_status)

            stmt = insert(StoreStatus).values(store_status_list).on_conflict_do_nothing()
            session.execute(stmt)
            session.commit()
    except IntegrityError as e:
        session.rollback()
        print(f"Error: {e}")
    except FileNotFoundError:
        print("File not found: store_status.csv")
