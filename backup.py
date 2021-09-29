"""
Maintainer: Maksym Maksymchuk
Email: maksymchuk.mm@gmail.com
"""

import gzip
import os
import shutil
import subprocess
from datetime import datetime

import boto3
import psycopg2
from dotenv import load_dotenv
from loguru import logger

from client import get_space_client, AWS_BUCKET_PATH
from utils import exist_dir

dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(dotenv_path)

logger.add(
    "logs/db.log",
    rotation="00:00",
    compression="zip",
    level="DEBUG"
)

DB_HOST = "localhost"
DB_PORT = 0000
DB_USER = "xxxxxxx"
DB_PASSWORD = os.environ.get("DB_PASSWORD_BACKUP")
DB_NAME = "postgres"
DB_SSL_MODE = "disable"

EXCLUDED_DB_NAMES = [
    "_dodb", "defaultdb", "benchmark",
]

BACKUP_DIR = "/tmp/main"
COMPRESS_BACKUP_DIR = "main"


@logger.catch
def get_connection(db_name=DB_NAME):
    conn = psycopg2.connect(dbname=db_name, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT)
    return conn


@logger.catch
def get_backup_file(db_name):
    now = datetime.now().strftime("%Y-%m-%d")
    exist_dir(BACKUP_DIR)
    return f"{BACKUP_DIR}/{db_name}_{now}.dump"


@logger.catch
def get_compress_file(db_name):
    now = datetime.now().strftime("%Y-%m-%d")
    exist_dir(COMPRESS_BACKUP_DIR, db_name)
    return f"{COMPRESS_BACKUP_DIR}/{db_name}/{db_name}_{now}.dump.gz"


@logger.catch
def remove_backup_file(db_name):
    os.remove(get_backup_file(db_name))


@logger.catch
def run():
    q = "SELECT datname FROM pg_database WHERE datistemplate = FALSE;"
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(q)

    # [('postgres',), ('vip_terminal',), ('vip_main',), ('vip_draw',), ('vip_dt_0',), ('vip_dt_1',), ('vip_dt_2',)]
    db_names = cursor.fetchall()
    cursor.close()
    conn.close()
    for db_name in db_names:
        name = db_name[0]
        if name not in EXCLUDED_DB_NAMES:
            backup_db(name)
            logger.info("Backup completed!")
            path_to_file = compress_file(name)
            logger.info("File compressed!")
            remove_backup_file(name)
            logger.info("Removed backup file!")
            upload_to_s3(path_to_file)


@logger.catch
def backup_db(db_name):
    logger.info(f"Creating backup for '{db_name}'")
    backup_file = get_backup_file(db_name)
    try:
        process = subprocess.Popen(
            ["pg_dump",
             f"--dbname=postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{db_name}?client_encoding=utf8&sslmode={DB_SSL_MODE}",
             "-Fc",
             # "-c", "--if-exists",  # drop db if exist, then a create new db
             "-f",
             backup_file,
             # '-v'
             ],
            stdout=subprocess.PIPE
        )

        output = process.communicate()[0]
        if int(process.returncode) != 0:
            logger.error(f"Command failed. Return code : {process.returncode}")
            exit(1)
        process.stdout.close()
        process.wait()
        return output
    except Exception as e:
        logger.error(e)
        exit(1)


@logger.catch
def compress_file(db_name):
    backup_file_name = get_backup_file(db_name)
    compress_file_name = get_compress_file(db_name)
    with open(backup_file_name, 'rb') as inp:
        with gzip.open(compress_file_name, 'wb') as out:
            shutil.copyfileobj(inp, out)
    return compress_file_name


@logger.catch
def upload_to_s3(file_path):
    s3_client = get_space_client()
    file_full_path = file_path
    dest_file = file_path
    logger.debug(f"Dest file: {dest_file}")
    try:
        s3_client.upload_file(file_full_path, AWS_BUCKET_PATH, dest_file)
        os.remove(file_full_path)
    except boto3.exceptions.S3UploadFailedError as exc:
        logger.critical(exc)
        exit(1)


if __name__ == '__main__':
    run()
