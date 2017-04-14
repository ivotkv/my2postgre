#!/usr/bin/env python
####
# Script to convert TINYINT to BOOLEAN for MySQL -> PostgreSQL.
#
# You should dump your MySQL data as follows:
#
# mysqldump --compatible=postgresql --no-create-info --complete-insert --extended-insert --skip-comments --skip-add-locks --default-character-set=utf8 --skip-tz-utc -u user -p database | sed -e "s/\\\'/''/g" > dump.sql
####

import re
import yaml
from sqlalchemy import create_engine
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.ext.declarative.api import DeclarativeMeta
from sqlalchemy.orm.attributes import InstrumentedAttribute
from sqlalchemy.orm.relationships import RelationshipProperty
from sqlalchemy.sql.sqltypes import BOOLEAN, DATE
from sqlalchemy.dialects.mysql.base import TINYINT, DATETIME, TIMESTAMP


class Database(object):

    def __init__(self, config):
        self.url = "{0}://{1}:{2}@{3}/{4}".format(config['type'],
                                                  config['user'],
                                                  config['pass'],
                                                  config['host'],
                                                  config['database'])
        self.engine = create_engine(self.url)
        self.automap = automap_base()
        self.automap.prepare(self.engine, schema=config.get('schema'), reflect=True)

    def get_table(self, name):
        table = getattr(self.automap.classes, name.lower(), None)
        assert isinstance(table, DeclarativeMeta), "Invalid table: {0}".format(name)
        return table

    def get_column_type(self, table, prop):
        if isinstance(table, basestring):
            table = self.get_table(table)
        assert isinstance(getattr(table, prop), InstrumentedAttribute), "Invalid property: {0}".format(prop)
        return getattr(table, prop).property.columns[0].type

    def is_boolean(self, table, prop):
        ctype = self.get_column_type(table, prop)
        return isinstance(ctype, BOOLEAN) or isinstance(ctype, TINYINT) and ctype.display_width == 1

    def is_datetime(self, table, prop):
        ctype = self.get_column_type(table, prop)
        return isinstance(ctype, (DATE, DATETIME, TIMESTAMP))


NEST_CHARS = {
    '\'': '\'',
    '"': '"',
    '(': ')'
}
NO_SUBNEST = {'\'', '"'}

def tokenize(string):
    tokens = []
    idx = 0
    token = ''
    context = []
    while idx < len(string):
        if string[idx] == '\'' and idx + 1 < len(string) and string[idx + 1] == '\'' :
            token += string[idx] + string[idx + 1]
            idx += 2
        elif len(context) == 0:
            if string[idx].isspace() or string[idx] == ',' or string[idx] == ';':
                if len(token) > 0:
                    tokens.append(token)
                    token = ''
                tokens.append(string[idx])
            else:
                if string[idx] in NEST_CHARS:
                    context.append(string[idx])
                token += string[idx]
            idx += 1
        else:
            if string[idx] == NEST_CHARS[context[-1]]:
                context.pop()
            elif context[-1] not in NO_SUBNEST and string[idx] in NEST_CHARS:
                context.append(string[idx])
            token += string[idx]
            idx += 1
    if len(token) > 0:
        tokens.append(token)
    if len(context) > 0:
        raise ValueError("Unclosed nestings in query: {0}".format(' '.join(context)))
    return tokens


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", help="config", type=str, default="config.yaml", dest="config")
    parser.add_argument("-i", help="input", type=str, required=True, dest="input")
    parser.add_argument("-o", help="output", type=str, required=True, dest="output")
    args = parser.parse_args()

    db = Database(yaml.load(file(args.config, 'r'))['mysql'])

    with open(args.input, 'r') as src:
        with open(args.output, 'w') as dst:
            dst.write("SET client_min_messages TO ERROR;\n")
            dst.write("SET standard_conforming_strings = 'off';\n")
            dst.write("SET backslash_quote = 'on';\n\n")
            dst.write("BEGIN;\n")
            dst.write("SET CONSTRAINTS ALL DEFERRED;\n\n")
            lcount = 0
            for line in src:
                lcount += 1
                rcount = 0
                tokens = tokenize(line.decode('utf-8'))
                if tokens[0] == 'INSERT':
                    try:
                        table = db.get_table(re.sub('"', '', tokens[4]))
                    except AssertionError as e:
                        print 'WARNING: AssertionError:', e
                    else:
                        tcols = [unicode(x) for x in dir(table) if isinstance(getattr(table, x), InstrumentedAttribute) and not isinstance(getattr(table, x).prop, RelationshipProperty)]
                        cols = [re.sub('"', '', x) for x in tokenize(tokens[6][1:-1]) if not (x.isspace() or x == ',')]
                        assert len(cols) == len(tcols), "Column count mismatch for line {0}:\n{1}\n{2}\n{3}".format(lcount, table, sorted(tcols), sorted(cols))
                        bools = [db.is_boolean(table, col) for col in cols]
                        has_bools = reduce(lambda x, y: x or y, bools)
                        dates = [db.is_datetime(table, col) for col in cols]
                        has_dates = reduce(lambda x, y: x or y, dates)
                        for i in range(8, len(tokens)):
                            if tokens[i][0] == '(':
                                rcount += 1
                                if has_bools or has_dates:
                                    rtokens = tokenize(tokens[i][1:-1])
                                    k = 0
                                    for j in range(len(rtokens)):
                                        if not (rtokens[j].isspace() or rtokens[j] == ','):
                                            if bools[k]:
                                                if rtokens[j] == '1':
                                                    #print cols[k], rtokens[j], 'TRUE'
                                                    rtokens[j] = 'TRUE'
                                                elif rtokens[j] == '0':
                                                    #print cols[k], rtokens[j], 'FALSE'
                                                    rtokens[j] = 'FALSE'
                                                else:
                                                    assert rtokens[j] == 'NULL', "Unexpected value for {0}: {1}".format(cols[k], rtokens[j])
                                            elif dates[k] and re.match("'0000-00-00", rtokens[j]):
                                                #print cols[k], rtokens[j], 'NULL'
                                                rtokens[j] = 'NULL'
                                            k += 1
                                    tokens[i] = '(' + u''.join(rtokens) + ')'
                dst.write(u''.join(tokens).encode('utf-8'))
                print lcount, rcount
            dst.write("\nCOMMIT;\n")
