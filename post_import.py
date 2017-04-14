#!/usr/bin/env python
####
# Script to fix PostgreSQL auto-increment counters after MySQL dump import
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

    def fix_autoincrement_counters(self):
        for table_name, table in self.automap.classes.items():
            for prop_name, prop in ((name, getattr(table, name)) for name in dir(table)):
                if isinstance(prop, InstrumentedAttribute) and not isinstance(prop.prop, RelationshipProperty):
                    if prop.property.columns[0].server_default and \
                       re.match('nextval', prop.property.columns[0].server_default.arg.text):
                        seq_name = prop.property.columns[0].server_default.arg.text.split('\'')[1]
                        print table_name, prop_name, seq_name
                        maxval = self.engine.execute("""SELECT max({0}) FROM {1}""".format(prop_name, table_name)).scalar() or 0
                        nextval = self.engine.execute("""SELECT setval('{0}', COALESCE((SELECT max("{1}") + 1 FROM "{2}"), 1), false)""".format(seq_name, prop_name, table_name)).scalar()
                        print maxval, nextval
                        assert nextval == maxval + 1

    def process(self):
        self.fix_autoincrement_counters()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", help="config", type=str, default="config.yaml", dest="config")
    args = parser.parse_args()

    db = Database(yaml.load(file(args.config, 'r'))['postgresql'])

    db.process()
