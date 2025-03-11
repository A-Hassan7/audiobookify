from dotenv import load_dotenv
import os

load_dotenv(".env")


class DBConfig:
    DRIVERNAME = os.getenv("DB_DRIVERNAME")
    USERNAME = os.getenv("DB_USERNAME")
    PASSWORD = os.getenv("DB_PASSWORD")
    HOST = os.getenv("DB_HOST")
    PORT = os.getenv("DB_PORT")
    DATABASE = os.getenv("DB_DATABASE")
