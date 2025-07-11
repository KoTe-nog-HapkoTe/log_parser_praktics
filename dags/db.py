import os
from sqlalchemy import create_engine, Column, Date, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgresql import ARRAY, BOOLEAN
from sqlalchemy.orm import sessionmaker
import pandas as pd

DATABASE_URL = "postgresql+psycopg2://postgres:{password}@host.docker.internal:{port}/log_analytics"
TABLE_NAME = "log_analytics"

Base = declarative_base()
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)

class LogAnalytics(Base):
    __tablename__ = TABLE_NAME
    date = Column(Date, primary_key=True)
    hour = Column(Integer, primary_key=True)
    user_name = Column(String, primary_key=True)
    request_count = Column(Integer, nullable=False)

class UserActivityCalendar(Base):
    __tablename__ = "user_activity_calendar"

    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(Date, nullable=False)
    user_name = Column(String, nullable=False)
    hour_calendar = Column(ARRAY(BOOLEAN, dimensions=1), nullable=False)

def create_tables():
    Base.metadata.create_all(engine)
    print(f"[db] Таблица '{TABLE_NAME}' проверена/создана.")

def upload_to_postgres_orm(df: pd.DataFrame):
    if df.empty:
        print("[db] Нет данных для загрузки.")
        return

    session = Session()
    try:
        for _, row in df.iterrows():
            existing = session.query(LogAnalytics).filter_by(
                date=row.date,
                hour=int(row.hour),
                user_name=row.user_name
            ).first()

            if existing:
                existing.request_count = row.request_count
            else:
                record = LogAnalytics(
                    date=row.date,
                    hour=int(row.hour),
                    user_name=row.user_name,
                    request_count=int(row.request_count)
                )
                session.add(record)

        session.commit()
        print(f"[db] Загружено строк: {len(df)}")
    except Exception as e:
        session.rollback()
        print(f"[db] ОШИБКА ORM: {e}")
    finally:
        session.close()

def upload_user_activity_calendar(df: pd.DataFrame):
    if df.empty:
        print("[calendar] Нет данных для загрузки.")
        return

    session = Session()
    try:
        for _, row in df.iterrows():
            record = UserActivityCalendar(
                date=row.date,
                user_name=row.user_name,
                hour_calendar=row.hour_calendar
            )
            session.add(record)

        session.commit()
        print(f"[calendar] Загружено строк: {len(df)}")
    except Exception as e:
        session.rollback()
        print(f"[calendar] ОШИБКА ORM: {e}")
    finally:
        session.close()
