# -*- coding: utf-8 -*-

import pytest
import time
import sqlparse

import pg_simple

INVALID_TOKENS = (sqlparse.tokens.Punctuation, sqlparse.tokens.Keyword, sqlparse.tokens.Whitespace, sqlparse.tokens.Comparison)

@pytest.fixture(scope="session")
def db():
    pg_simple.PgSimple._connect = lambda x: 0
    return pg_simple.PgSimple(pool=None)

@pytest.mark.parametrize("data", [ {'key_1': 'value_1'}, {'key_1': 'value_1', 'key_2': 'value_2'}, ])
def test_insert(db, data):
    sql = db._insert('my_table', data).lower().strip()

    tokens = sqlparse.parse(sql)[0].flatten()
    valid_tokens = [t for t in tokens if not t.ttype in INVALID_TOKENS]

    assert valid_tokens.pop(0).value == 'insert'
    assert valid_tokens.pop(0).value == 'my_table'

    for key in data.keys():
        assert valid_tokens.pop(0).value == key

    for value in data.values():
        # properly tokenized
        assert valid_tokens.pop(0).ttype == sqlparse.tokens.Name.Placeholder
    assert len(valid_tokens) == 0

@pytest.mark.parametrize("data", [
    {'key_1': 'value_1'}, 
    {'key_1': 'value_1', 'key_2': 'value_2'},
    {'key_1': 'value_1', 'key_2': 'value_2'},
    ])
@pytest.mark.parametrize("where", [
    ('condition = %s', ["77"]), 
    ('condition = %s and condition2 = %s', ["77", "88"]),
    ])
def test_update(db, data, where):
    sql = db._update('my_table', data=data, w_clause=where[0], returning=None)

    tokens = sqlparse.parse(sql)[0].flatten()
    valid_tokens = [t for t in tokens if not t.ttype in INVALID_TOKENS]
    assert valid_tokens.pop(0).value.lower() == 'update'
    assert valid_tokens.pop(0).value == 'my_table'

    for key in data.keys():
        assert valid_tokens.pop(0).value == key
        assert valid_tokens.pop(0).ttype == sqlparse.tokens.Name.Placeholder

    if where:
        condition, values = where
        for value in values:
            assert valid_tokens.pop(0).ttype == sqlparse.tokens.Name # how do I test condition/condition2
            assert valid_tokens.pop(0).ttype == sqlparse.tokens.Name.Placeholder # properly tokenized
    assert len(valid_tokens) == 0


@pytest.mark.parametrize("tables,fields,join_fields", [
    (('t1', 't2'), (('t1_f1', 't1_f2'), ('t2_f1', 't2_f2',)), ('t1_f2','t2_f1')),
    ])
def test_join(db, tables, fields, join_fields):
    sql = db._join_sql(tables, fields, join_fields, '', None, None, None).lower().strip()
    assert f'from {tables[0]} left join {tables[1]}' in sql
    tokens = sqlparse.parse(sql)[0].flatten()
    valid_tokens = [t for t in tokens if not t.ttype in INVALID_TOKENS]

    assert valid_tokens.pop(0).value.lower() == 'select'
    i = 0
    # select t_k.f_j ...
    for table in tables:
        for field in fields[i]:
            assert valid_tokens.pop(0).value == table # <Ti>.Fi
            assert valid_tokens.pop(0).value == field # Ti.<Fi>
        i+= 1

    # from TABLE left join TABLEi
    for table in tables:
        assert valid_tokens.pop(0).value == table

    # on Ti.Fk = Tj.Fn
    i = 0
    for table in tables:
        assert valid_tokens.pop(0).value == table # <Ti>.Fi
        assert valid_tokens.pop(0).value == join_fields[i] # Ti.<Fi>
        i+= 1

    assert len(valid_tokens) == 0
