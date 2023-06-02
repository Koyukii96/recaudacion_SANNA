import os
from typing import Dict, Optional
from sqlalchemy import text, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine.url import URL

Base = declarative_base()

class DatabaseConnection:
    def __init__(self, db_type: str, db_info: Dict[str, str], trust_connection: bool = False):
        self.db_type = db_type.lower()
        self.db_info = db_info
        self.trust_connection = trust_connection
        self.engine = self._create_engine()
        self.Session = sessionmaker(bind=self.engine)

    def _create_engine(self):
        if self.db_type == "sqlserver":
            return self._create_sqlserver_engine()
        elif self.db_type == "mysql":
            return self._create_mysql_engine()
        elif self.db_type == "postgres":
            return self._create_postgres_engine()
        elif self.db_type == "sqlite":
            return self._create_sqlite_engine()
        else:
            raise ValueError("Invalid database type.")

    def _create_sqlserver_engine(self):
        url_params = {
            "drivername": "mssql+pyodbc",
            "username": self.db_info["username"],
            "password": self.db_info["password"],
            "host": self.db_info["host"],
            "port": self.db_info["port"],
            "database": self.db_info["database"],
            "query": {"driver": "ODBC Driver 17 for SQL Server"},
        }

        if self.trust_connection:
            url_params["query"]["trusted_connection"] = "yes"

        return create_engine(URL.create(**url_params), pool_size=20, max_overflow=0)

    def _create_mysql_engine(self):
        return create_engine(
            f"mysql+pymysql://{self.db_info['username']}:{self.db_info['password']}@"
            f"{self.db_info['host']}:{self.db_info['port']}/{self.db_info['database']}?charset=utf8mb4"
        )

    def _create_postgres_engine(self):
        return create_engine(
            f"postgresql+psycopg2://{self.db_info['username']}:{self.db_info['password']}@"
            f"{self.db_info['host']}:{self.db_info['port']}/{self.db_info['database']}"
        )

    def _create_sqlite_engine(self):
        return create_engine(f"sqlite:///{self.db_info['file']}")

    def execute_query(self, query: str, parameters: Optional[Dict[str, str]] = None):
        with self.Session() as session:
            result = session.execute(query, parameters)
            session.commit()
            return result.fetchall()

    def execute_stored_procedure(self, sp_name: str, parameters: Optional[Dict[str, str]] = None):
        with self.Session() as session:
            result = session.execute(text(f"EXEC {sp_name}"), parameters)
            session.commit()
            return result.fetchall()
    
    def execute_stored_procedure_nreturn(self, sp_name: str, parameters: Optional[Dict[str, str]] = None):
        with self.Session() as session:
            result = session.execute(text(f"EXEC {sp_name}"), parameters)
            session.commit()
            return result
