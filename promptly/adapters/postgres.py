import loguru
from sqlalchemy import create_engine, text

logger = loguru.logger


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

    def configure_user_cdc(self):
        # ALTER SYSTEM commands require superuser privileges
        with self.engine.connect().execution_options(
            isolation_level='AUTOCOMMIT'
        ) as connection:
            # TODO: create a proper user to execute these commands
            connection.execute(
                text(f'ALTER ROLE {self.user} WITH REPLICATION;')
            )
            connection.execute(text(f'GRANT pg_read_all_data TO {self.user};'))
            connection.commit()

        logger.info('CDC enabled on PostgreSQL database.')

    def is_cdc_enabled(self) -> bool:
        with self.engine.connect() as connection:
            result = connection.execute(text('SHOW wal_level;'))
            wal_level = result.scalar()
            return wal_level == 'logical'

    def create_publication_for_table(self, table: str):
        with self.engine.connect() as connection:
            connection.execute(
                text(f"""
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1
                        FROM pg_publication
                        WHERE pubname = '{table}_publication'
                    ) THEN
                        CREATE PUBLICATION {table}_publication FOR TABLE {table};
                    END IF;
                END$$;
            """)  # noqa: E501
            )
            connection.commit()

    def disable_cdc(self):
        with self.engine.connect() as connection:
            connection.execute(text('ALTER SYSTEM SET wal_level = replica;'))
            connection.execute(text('SELECT pg_reload_conf();'))
            connection.commit()
