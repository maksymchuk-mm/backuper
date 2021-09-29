# Backuper

### For using this scripts you need a create variables on environment:
```bash
$ export AWS_ACCESS_KEY_ID=
$ export AWS_SECRET_ACCESS_KEY=
$ export DB_RES_PASSWORD=
$ export DB_PASSWORD_BACKUP=
```

### Example:
#### Create backups for all database on instance
```bash
$ python backup.py
```
#### Recovery all database from backups by created date
```bash
$ python restore.py -d 2021-9-29
```
#### Recovery database from backups by created date and prefix for database (ex: shard_1 .. shard_N)
```bash 
$ python restore.py -d 2021-9-29 -pr <your db prefix>
```