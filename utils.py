import os

from loguru import logger


@logger.catch
def sizeof_fmt(num, suffix='B'):
    for unit in ['', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)


@logger.catch
def exist_dir(path, name_folder=None):
    if name_folder is not None:
        path = "".join([path, '/', name_folder])
    if not os.path.exists(path):
        logger.info(f"Create new dir {path}")
        os.makedirs(path, exist_ok=True)
