from sqlalchemy import create_engine, text


class HealthCareDB:
    def __init__(
        self,
        user: str,
        password: str,
        host: str,
        port: int,
        db_name: str,
    ):
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.db_name = db_name
        self.engine = create_engine(
            f'postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.db_name}'
        )

    def execute_query(self, query: str):
        with self.engine.connect() as connection:
            result = connection.execute(text(query))
            return result.fetchall()

    def close(self):
        self.engine.dispose()
