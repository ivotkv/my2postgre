# MySQL to PostgreSQL Migration

Follow these steps to migrate data from MySQL to PostgreSQL:

Make sure you have the required dependencies:
```
pip install PyYAML
pip install sqlalchemy
```

Next, make a `config.yaml` and fill in info for both databases:
```
cp config.yaml.example config.yaml
vi config.yaml
```

Run pre-import script (truncates `migrate_version` table and makes PostgreSQL constraints deferrable):
```
./pre_import.py
```

Create a new `mysqldump`:
```
mysqldump --compatible=postgresql --no-create-info --complete-insert --extended-insert --skip-comments --skip-add-locks --default-character-set=utf8 --skip-tz-utc -u [user] -p [database] | sed -e "s/\\\'/''/g" > dump.sql
```

Run script to process the dump for `psql`:
```
./my2postgre.py -i dump.sql -o fixed.sql
```

Import the data with `psql`:
```
psql -U [user] -W [database] -f fixed.sql
```

Run post-import script (fixes auto-increment counters):
```
./post_import.py
```

Run script to compare MySQL and PostgreSQL data to validate migration:
```
./validate_migration.py [-r]
```
(the `-r` will validate ManyToMany relationships, optional since it may be more time-consuming)
