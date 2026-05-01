from dagster import ConfigurableResource
from sqlalchemy import create_engine, Engine


class HiveResource(ConfigurableResource):
    """Resource para conectarse a Hive usando SQLAlchemy"""
    
    hive_ip: str
    username: str
    database: str
    port: int = 10000  # Puerto por defecto de Hive Thrift Server

    def get_engine(self) -> Engine:
        """Crea y retorna un SQLAlchemy Engine para la conexión a Hive"""
        connection_string = f"hive://{self.username}@{self.hive_ip}:{self.port}/{self.database}"
        return create_engine(connection_string)
