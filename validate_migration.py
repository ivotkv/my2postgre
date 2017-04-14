#!/usr/bin/env python
####
# Script to validate PostgreSQL data matches original MySQL data
####

import re
import yaml
from sqlalchemy import create_engine
from sqlalchemy.ext import automap
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.ext.declarative.api import DeclarativeMeta
from sqlalchemy.orm.attributes import InstrumentedAttribute
from sqlalchemy.orm.relationships import RelationshipProperty
from sqlalchemy.orm.collections import InstrumentedList
from sqlalchemy.orm.session import sessionmaker
from sqlalchemy.orm.mapper import Mapper

def name_for_collection_relationship(base, local_cls, referred_cls, constraint):
    # TODO: this should be made customizable
    if constraint.name == 'prev_fk':
        return 'prev_collection'
    elif constraint.name == 'next_fk':
        return 'next_collection'
    else:
        return automap.name_for_collection_relationship(base, local_cls, referred_cls, constraint)

class Database(object):

    def __init__(self, config):
        self.url = "{0}://{1}:{2}@{3}/{4}".format(config['type'],
                                                  config['user'],
                                                  config['pass'],
                                                  config['host'],
                                                  config['database'])
        if config['type'] == 'mysql':
            self.url += '?charset=utf8'
        self.engine = create_engine(self.url)
        self.automap = automap_base()
        self.automap.prepare(self.engine, schema=config.get('schema'), reflect=True,
                             name_for_collection_relationship=name_for_collection_relationship)
        self.sessionmaker = sessionmaker(bind=self.engine, autoflush=False)

    @property
    def session(self):
        try:
            return self._session
        except AttributeError:
            self._session = self.sessionmaker()
            return self._session

    def process(self, other, check_rels=False):
        # configure all relationships
        Mapper._configure_all()

        for table_name, table in self.automap.classes.items():
            other_table = getattr(other.automap.classes, table_name)
            assert isinstance(other_table, type(table)), table_name
            assert dir(table) == dir(other_table), (table_name, dir(table), dir(other_table))

            # get columns
            m2m_rels = set()
            prop_names = set()
            for prop_name, prop in ((name, getattr(table, name)) for name in dir(table)):
                if isinstance(prop, InstrumentedAttribute):
                    other_prop = getattr(other_table, prop_name)
                    assert isinstance(other_prop, type(prop)), (table_name, prop_name)
                    if isinstance(prop.property, RelationshipProperty):
                        if prop.property.direction.name == 'MANYTOMANY':
                            rel_pk = list(prop.property.table.primary_key.columns)[0].name
                            m2m_rels.add((prop_name, rel_pk))
                    else:
                        prop_names.add(prop_name)

            # check counts
            my_count = self.session.query(table).count()
            other_count = other.session.query(other_table).count()
            assert my_count == other_count, (table_name, my_count, other_count)

            # determine primary key
            assert len(table.__table__.primary_key.columns) == 1
            assert len(other_table.__table__.primary_key.columns) == 1
            table_pk = list(table.__table__.primary_key.columns)[0].name
            assert list(other_table.__table__.primary_key.columns)[0].name == table_pk
            assert table_pk in prop_names

            # compare values
            offset = 0
            limit = 100
            my_entities = self.session.query(table).order_by(getattr(table, table_pk)).offset(offset).limit(limit).all()
            other_entities = other.session.query(other_table).order_by(getattr(other_table, table_pk)).offset(offset).limit(limit).all()
            print table_name, '0', '/', my_count
            while len(my_entities) > 0:
                for idx in range(len(my_entities)):
                    assert my_entities[idx]._sa_instance_state.session is self.session
                    assert other_entities[idx]._sa_instance_state.session is other.session
                    for prop_name in prop_names:
                        my_prop = getattr(my_entities[idx], prop_name)
                        my_type = type(my_prop)
                        other_prop = getattr(other_entities[idx], prop_name)
                        other_type = type(other_prop)
                        if other_type is bool:
                            assert my_type is int
                            assert my_prop in (0, 1)
                            assert bool(my_prop) == other_prop
                        else:
                            if my_type is long:
                                assert other_type in (long, int), (prop_name, my_type, other_type)
                            else:
                                assert my_type is other_type, (prop_name, my_type, other_type)
                            assert my_prop == other_prop, (prop_name, my_prop, other_prop)
                    if check_rels:
                        for rel_name, rel_pk in m2m_rels:
                            my_rel = getattr(my_entities[idx], rel_name)
                            other_rel = getattr(other_entities[idx], rel_name)
                            assert type(my_rel) == type(other_rel) == InstrumentedList, rel_name
                            my_rel = list(sorted(my_rel, key=lambda x: getattr(x, rel_pk)))
                            other_rel = list(sorted(other_rel, key=lambda x: getattr(x, rel_pk)))
                            assert len(my_rel) == len(other_rel), rel_name
                            for idx in range(len(my_rel)):
                                assert type(my_rel[idx]) is not type(other_rel[idx]), rel_name
                                assert my_rel[idx].__table__.name == other_rel[idx].__table__.name, rel_name
                                assert getattr(my_rel[idx], rel_pk) == getattr(other_rel[idx], rel_pk), rel_name
                print table_name, offset + len(my_entities), '/', my_count
                self.session.close()
                other.session.close()
                offset += limit
                my_entities = self.session.query(table).order_by(getattr(table, table_pk)).offset(offset).limit(limit).all()
                other_entities = other.session.query(other_table).order_by(getattr(other_table, table_pk)).offset(offset).limit(limit).all()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", help="config", type=str, default="config.yaml", dest="config")
    parser.add_argument("-r", help="validate relationships", action="store_true", dest="check_rels")
    args = parser.parse_args()

    mysql = Database(yaml.load(file(args.config, 'r'))['mysql'])
    postgresql = Database(yaml.load(file(args.config, 'r'))['postgresql'])

    mysql.process(postgresql, check_rels=args.check_rels)
