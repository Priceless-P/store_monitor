import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, DateTime, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

database_url = "postgresql+psycopg2://wpdnwwtb:cFlMv1ea-WkkH2AuYLKe7ywr7TiCWXU3@heffalump.db.elephantsql.com/wpdnwwtb"
database_engine = create_engine(database_url, client_encoding='utf8', echo=True)

Session = sessionmaker(bind=database_engine)
session = Session()
Base = declarative_base()


class StoreStatus(Base):
    __tablename__ = 'store_status'
    id = Column(Integer, primary_key=True)
    store_id = Column(String, nullable=False)
    status = Column(String, nullable=False)
    timestamp_utc = Column(DateTime, nullable=False)


class StoreHours(Base):
    __tablename__ = 'store_hours'
    id = Column(Integer, primary_key=True)
    store_id = Column(String, nullable=False)
    day_of_week = Column(Integer, nullable=False)
    start_time_local = Column(String, nullable=True)
    end_time_local = Column(String, nullable=True)


class StoreTimeZone(Base):
    __tablename__ = 'store_timezone'
    id = Column(Integer, primary_key=True)
    store_id = Column(String, nullable=False)
    timezone = Column(String, nullable=True)


def get_hrs(store_id):
    hours_query = text(
        'SELECT store_id, day_of_week, start_time_local, end_time_local '
        'FROM store_hours WHERE store_id = :store_id ORDER BY day_of_week'
    )
    store_hours = pd.read_sql_query(hours_query, con=database_engine, params={"store_id": store_id})
    store_hours = store_hours.astype(
        {'store_id': str, 'day_of_week': str, 'start_time_local': str, 'end_time_local': str})
    return store_hours.to_dict('records')


def get_store_timezone(store_id):
    with Session():
        query = text('SELECT store_id, timezone FROM store_timezone WHERE store_id= :store_id')
        df = pd.read_sql_query(query, con=database_engine, params={"store_id": store_id})
        return df.to_dict('records')


def get_timestamps(store_id):
    with Session():
        query = text('SELECT status, timestamp_utc FROM store_status WHERE store_id = :store_id')
        df = pd.read_sql_query(query, con=database_engine, params={"store_id": store_id})
        return df


def get_max_timestamp():
    with Session():
        query = text('SELECT Max(timestamp_utc) FROM store_status')
        df = pd.read_sql_query(query, con=database_engine)
        return df


def get_all_stores():
    query = text('SELECT store_id FROM store_timezone')
    df = pd.read_sql_query(query, con=database_engine)
    return df


def get_times(store_id):
    with Session():
        query = text('SELECT start_time_local, end_time_local FROM store_hours WHERE store_id = :store_id')
        df = pd.read_sql_query(query, con=database_engine, params={"store_id": store_id})
        return df
