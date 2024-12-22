from dataclasses import dataclass
from environs import Env
from typing import Union


@dataclass
class DBConnection:
    database: str
    host: str
    user: str
    password: str
    port: str


@dataclass
class Config:
    db_connect: DBConnection


def load_config(path: Union[str, None] = None) -> Config:

    env: Env = Env()
    env.read_env(path)

    return Config(
        db_connect=DBConnection(
            database=env('DB_NAME'),
            host=env('DB_HOST'),
            user=env('DB_USER'),
            password=env('DB_PASSWORD'),
            port=env('DB_PORT')
        )
    )
