#!/usr/bin/env python
####
# Script to make PostgreSQL constraints deferrable to avoid foreign key issues
####

import re
import yaml
from sqlalchemy import create_engine
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.ext.declarative.api import DeclarativeMeta
from sqlalchemy.orm.attributes import InstrumentedAttribute
from sqlalchemy.orm.relationships import RelationshipProperty

class Database(object):

    def __init__(self, config):
        self.url = "{0}://{1}:{2}@{3}/{4}".format(config['type'],
                                                  config['user'],
                                                  config['pass'],
                                                  config['host'],
                                                  config['database'])
        self.engine = create_engine(self.url, isolation_level='AUTOCOMMIT')
        self.automap = automap_base()
        self.automap.prepare(self.engine, schema=config.get('schema'), reflect=True)

    def execute(self, sql):
        print sql
        self.engine.execute(sql)

    def truncate_migrate_table(self):
        self.execute("""TRUNCATE TABLE "migrate_version";""")

    def make_constraints_deferrable(self):
        for table_name, table in self.automap.classes.items():
            for prop_name, prop in ((name, getattr(table, name)) for name in dir(table)):
                if isinstance(prop, InstrumentedAttribute) and not isinstance(prop.prop, RelationshipProperty):
                    for fk in prop.property.columns[0].foreign_keys:
                        fk_name = fk.constraint.name
                        self.execute("""ALTER TABLE "{0}" ALTER CONSTRAINT "{1}" DEFERRABLE;""".format(table_name, fk_name))

    def process(self):
        self.truncate_migrate_table()
        self.make_constraints_deferrable()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", help="config", type=str, default="config.yaml", dest="config")
    args = parser.parse_args()

    db = Database(yaml.load(file(args.config, 'r'))['postgresql'])

    db.process()
