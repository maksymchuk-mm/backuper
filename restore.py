"""
Maintainer: Maksym Maksymchuk
Email: maksymchuk.mm@gmail.com
"""
import argparse
import gzip
import os
import shutil
import subprocess
from datetime import datetime

import psycopg2
from dotenv import load_dotenv
from loguru import logger
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from client import get_space_client, AWS_BUCKET_PATH
from utils import exist_dir, sizeof_fmt

dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(dotenv_path)

logger.add(
    "logs/restore.log",
    rotation="00:00",
    compression="zip",
    level="INFO"
)

DB_HOST = "localhost"
DB_PORT = 0000
DB_USER = "xxxxxxxx"
DB_PASSWORD = os.environ.get("DB_RES_PASSWORD")
DB_NAME = "postgres"
DB_SSL_MODE = "disable"  # disable, allow, prefer, require, verify-ca, verify-full
DB_PREFIX = "postgres"

BACKUP_DIR = "/tmp"
COMPRESS_BACKUP_DIR = "gzip"


@logger.catch
def download(client, space_file):
    folder, db_name = space_file.split('/')[:2]
    exist_dir(folder, db_name)
    client.download_file(
        AWS_BUCKET_PATH,
        space_file,
        space_file
    )
    return space_file


@logger.catch
def unpack(local_path_file):
    unpack_file = local_path_file.split('.gz')[0]
    with gzip.open(local_path_file, 'rb') as input_file:
        with open(unpack_file, 'wb') as output_file:
            shutil.copyfileobj(input_file, output_file)
    return unpack_file


@logger.catch
def restore(dump_file):
    db_name = dump_file.split('/')[1]
    create_db(db_name)
    try:
        process = subprocess.Popen(
            ["pg_restore",
             '--no-owner',
             f"--dbname=postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{db_name}?client_encoding=utf8&sslmode={DB_SSL_MODE}",
             dump_file,
             ],
            stdout=subprocess.PIPE
        )
        output = process.communicate()[0]
        if int(process.returncode) != 0:
            logger.error(f'Command failed. Return code : {process.returncode}')

        return output
    except Exception as e:
        logger.critical(e)
        exit(1)


@logger.catch
def create_db(db_name):
    try:
        conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT)
    except Exception as e:
        logger.critical(e)
        exit(1)
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()
    try:
        cur.execute(f'''DROP DATABASE "{db_name}" ;''')
    except Exception as e:
        logger.error("DB does not exist, nothing to drop")
    cur.execute(f'''CREATE DATABASE "{db_name}" ENCODING='UTF8' LC_COLLATE='en_US.utf8' LC_CTYPE='en_US.utf8' TEMPLATE = template0; ''')
    cur.execute(f'''GRANT ALL PRIVILEGES ON DATABASE "{db_name}" TO {DB_USER} ;''')


@logger.catch
def run(client, date, prefix=None):
    logger.debug(f"Run {date}")
    if prefix is None:
        prefix = DB_PREFIX

    for i in client.list_objects(Bucket=AWS_BUCKET_PATH)['Contents']:
        file_in_space = i['Key']
        if str(file_in_space).endswith("gz"):
            s = file_in_space.split('/')[-1]
            if str(s).startswith(prefix):
                logger.debug(f"date modify: {i['LastModified'].date()}")
                if i['LastModified'].date() == date:
                    logger.info(f"File: {file_in_space}, Size: {sizeof_fmt(int(i['Size']))}")
                    sql = unpack(download(client, file_in_space))
                    restore(sql)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Data restoring tool')
    parser.add_argument('-d', '--date', help='Use data from that date. Format: YYYY-mm-dd', required=True)
    parser.add_argument('-pr', '--prefix', help='Prefix for name database. Example: -pr shard_', required=False)
    # parser.add_argument('-o', '--owner', help='Set owner dor database', required=True)
    args = parser.parse_args()

    s3_client = get_space_client()

    try:
        date = datetime.strptime(args.date, "%Y-%m-%d").date()
    except ValueError:
        logger.error("Invalid format of date argument. Supplied %s, doesn't match %%Y-%%m-%%d", args.date)
        exit(1)
    else:
        run(s3_client, date, args.prefix)

