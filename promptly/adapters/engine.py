# trino_adapter.py
import loguru
from trino.dbapi import connect

logger = loguru.logger


class TrinoCluster:
    def __init__(
        self,
        host: str,
        port: int,
        user: str,
    ):
        self.conn = connect(
            host=host,
            port=port,
            user=user,
        )

    def list_catalogs(self):
        with self.conn.cursor() as cursor:
            cursor.execute('SHOW CATALOGS')
            catalogs = [row[0] for row in cursor.fetchall()]
        return catalogs

    def create_catalog_if_not_exists(
        self,
        catalog_name: str,
        creation_sql: str,
    ):
        with self.conn.cursor() as cursor:
            if catalog_name not in self.list_catalogs():
                cursor.execute(creation_sql)
                self.conn.commit()
                logger.info(
                    f"Catalog '{catalog_name}' created successfully in Trino."
                )

            else:
                logger.info(
                    f"Catalog '{catalog_name}' already exists in Trino."
                )
            cursor.close()

    def execute_query(self, query: str):
        with self.conn.cursor() as cursor:
            cursor.execute(query)
            results = cursor.fetchall()
        return results

    def close(self):
        self.conn.close()
