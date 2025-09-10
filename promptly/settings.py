import os

from pydantic import BaseModel

from promptly.adapters.engine import TrinoCluster
from promptly.adapters.postgres import HealthCareDB
from promptly.adapters.s3 import MinioS3


class Settings(BaseModel):
    health_care_db: HealthCareDB
    trino_cluster: TrinoCluster
    s3: MinioS3

    class Config:
        arbitrary_types_allowed = True


def configure_settings():
    minio = MinioS3(
        endpoint_url=os.getenv('MINIO_ENDPOINT', 'localhost:9000'),
        access_key=os.getenv('MINIO_ACCESS_KEY', 'minioadmin'),
        secret_key=os.getenv('MINIO_SECRET_KEY', 'minioadmin'),
        secure=False,
    )

    health_care_db = HealthCareDB(
        user=os.getenv('HEALTH_CARE_DB_POSTGRES_USER', 'test'),
        password=os.getenv('HEALTH_CARE_DB_POSTGRES_PASSWORD', 'test'),
        host=os.getenv('HEALTH_CARE_DB_POSTGRES_HOST', 'localhost'),
        port=os.getenv('HEALTH_CARE_DB_POSTGRES_PORT', '5434'),
        db_name=os.getenv('HEALTH_CARE_DB_POSTGRES_DB', 'test'),
    )

    trino_cluster = TrinoCluster(
        host=os.getenv('TRINO_HOST', 'localhost'),
        port=os.getenv('TRINO_PORT', '8080'),
        user=os.getenv('TRINO_USER', 'test'),
    )

    settings = Settings(
        health_care_db=health_care_db,
        s3=minio,
        trino_cluster=trino_cluster,
    )

    return settings
