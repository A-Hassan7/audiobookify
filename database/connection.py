from config import DBConfig

from sqlalchemy.engine import URL
from sqlalchemy import create_engine


class DatabaseEngine:
    """
    Singleton class for the matrix database engine
    """

    _engine = None

    def __new__(cls):
        if cls._engine is None:

            cls._engine = create_engine(
                URL.create(
                    drivername=DBConfig.DRIVERNAME,
                    host=DBConfig.HOST,
                    port=DBConfig.PORT,
                    username=DBConfig.USERNAME,
                    password=DBConfig.PASSWORD,
                    database=DBConfig.DATABASE,
                )
            )

        return cls._engine
