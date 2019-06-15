# -*- coding: utf-8 -*-
__author__ = 'Masroor Ehsan'

import time
from collections import namedtuple
import logging
import os

from enum import Enum
from psycopg2.extras import DictCursor, NamedTupleCursor

class Order(Enum):
    DESC = 'DESC'
    ASC = 'ASC'


class PgSimple(object):
    _connection = None
    _cursor = None
    _log = None
    _log_fmt = None
    _cursor_factory = None
    _pool = None

    def __init__(self, pool, log=None, log_fmt=None, nt_cursor=True):
        self._log = log
        self._log_fmt = log_fmt
        self._cursor_factory = NamedTupleCursor if nt_cursor else DictCursor
        self._pool = pool
        self._connect()

    def _connect(self):
        """Connect to the postgres server"""
        try:
            self._connection = self._pool.get_conn()
            self._cursor = self._connection.cursor(cursor_factory=self._cursor_factory)
        except Exception as e:
            self._log_error('postgresql connection failed: ' + e.message)
            raise

    def _debug_write(self, msg):
        if msg and self._log:
            if isinstance(self._log, logging.Logger):
                self._log.debug(msg)
            else:
                self._log.write(msg + os.linesep)

    def _log_cursor(self, cursor):
        if not self._log:
            return

        if self._log_fmt:
            msg = self._log_fmt(cursor)
        else:
            msg = str(cursor.query)

        self._debug_write(msg)

    def _log_error(self, data):
        if not self._log:
            return

        if self._log_fmt:
            msg = self._log_fmt(data)
        else:
            msg = str(data)

        self._debug_write(msg)

    def fetchone(self, table, fields='*', where=None, order=None, offset=None):
        """Get a single result

            table = (str) table_name
            fields = (field1, field2 ...) list of fields to select
            where = ("parameterized_statement", [parameters])
                    eg: ("id=%s and name=%s", [1, "test"])
            order = [field, ASC|DESC]
        """
        cur = self._select(table, fields, where, order, 1, offset)
        return cur.fetchone()

    def fetchall(self, table, fields='*', where=None, order=None, limit=None, offset=None):
        """Get all results

            table = (str) table_name
            fields = (field1, field2 ...) list of fields to select
            where = ("parameterized_statement", [parameters])
                    eg: ("id=%s and name=%s", [1, "test"])
            order = [field, ASC|DESC]
            limit = [limit, offset]
        """
        cur = self._select(table, fields, where, order, limit, offset)
        return cur.fetchall()

    def join(self, tables=(), fields=(), join_fields=(), where=None, order=None, limit=None, offset=None):
        """Run an inner left join query

            tables = (table1, table2)
            fields = ([fields from table1], [fields from table 2])  # fields to select
            join_fields = (field1, field2)  # fields to join. field1 belongs to table1 and field2 belongs to table 2
            where = ("parameterized_statement", [parameters])
                    eg: ("id=%s and name=%s", [1, "test"])
            order = [field, ASC|DESC]
            limit = [limit1, limit2]
        """
        cur = self._join(tables, fields, join_fields, where, order, limit, offset)
        result = cur.fetchall()

        rows = None
        if result:
            Row = namedtuple('Row', [f[0] for f in cur.description])
            rows = [Row(*r) for r in result]

        return rows

    def _insert(self, table, data, returning=None):
        """Insert a record"""
        cols, val_placeholders = self._format_insert(data.keys())
        returning = self._returning(returning)
        sql = f'INSERT INTO {table} ({cols}) VALUES({val_placeholders}) {returning}'
        return sql

    def insert(self, table, data, returning=None):
        sql = self._insert(table, data, returning)
        cur = self.execute(sql, list(data.values()))
        return cur.fetchone() if returning else cur.rowcount

    def _update(self, table, data, w_clause, returning):
        """Insert a record"""
        new_values = self._format_update(data.keys())
        returning = self._returning(returning)

        sql = f'UPDATE {table} SET {new_values} {w_clause} {returning}'
        return sql

    def update(self, table, data, where=None, returning=None):
        assert len(data.keys()) > 0
        w_clause, w_values = self._where(where)
        sql = self._update(table, data, w_clause, returning)
        cur = self.execute(sql, list(data.values()) + w_values)

        return cur.fetchall() if returning else cur.rowcount

    def delete(self, table, where=None, returning=None):
        """Delete rows based on a where condition"""
        w_clause, w_values = self._where(where)
        sql = _delete(table, w_clause, returning)
        returning = self._returning(returning)
        sql = f'DELETE FROM {table} {w_clause} {returning}'
        cur = self.execute(sql, w_values)
        return cur.fetchall() if returning else cur.rowcount

    def execute(self, sql, params=None):
        """Executes a raw query"""
        try:
            if self._log and self._log_fmt:
                self._cursor.timestamp = time.time()
            self._cursor.execute(sql, params)
            if self._log and self._log_fmt:
                self._log_cursor(self._cursor)
        except Exception as e:
            if self._log and self._log_fmt:
                self._log_error('execute() failed: ' + e.message)
            raise

        return self._cursor

    def truncate(self, table, restart_identity=False, cascade=False):
        """Truncate a table or set of tables

        db.truncate('tbl1')
        db.truncate('tbl1, tbl2')
        """
        sql = 'TRUNCATE %s'
        if restart_identity:
            sql += ' RESTART IDENTITY'
        if cascade:
            sql += ' CASCADE'
        self.execute(sql % table)

    def drop(self, table, cascade=False):
        """Drop a table"""
        sql = 'DROP TABLE IF EXISTS %s'
        if cascade:
            sql += ' CASCADE'
        self.execute(sql % table)

    def create(self, table, schema):
        """Create a table with the schema provided

        pg_db.create('my_table','id SERIAL PRIMARY KEY, name TEXT')"""
        self.execute('CREATE TABLE %s (%s)' % (table, schema))

    def commit(self):
        """Commit a transaction"""
        return self._connection.commit()

    def rollback(self):
        """Roll-back a transaction"""
        return self._connection.rollback()

    @property
    def is_open(self):
        """Check if the connection is open"""
        return self._connection.open

    def _format_insert(self, data):
        """Format insert dict KEYS into strings"""
        cols = ",".join(data)
        vals = ",".join(["%s"]*len(data))

        return cols, vals

    def _format_update(self, data):
        """Format update dict KEYS into string"""
        return "=%s,".join(data) + "=%s"

    def _where(self, where=None):
        if where:
            assert len(where) == 2
            return 'WHERE %s' % where[0], where[1]
        return '', []

    def _order(self, order=None):
        sql = ''
        if order:
            assert len(order) <= 2
            sql += ' ORDER BY %s' % order[0]
            if len(order) > 1:
                assert type(order[1]) is Order
                sql += ' %s' % order[1].value
        return sql

    def _limit(self, limit):
        if limit:
            return ' LIMIT %d' % limit
        return ''

    def _offset(self, offset):
        if offset:
            return ' OFFSET %d' % offset
        return ''

    def _returning(self, returning):
        if returning:
            return ' RETURNING %s' % returning
        return ''

    def _select(self, table=None, fields=(), where=None, order=None, limit=None, offset=None):
        """Run a select query"""
        fields = ",".join(fields)
        w_clause, w_values = self._where(where)
        order = self._order(order)
        limit = self._limit(limit)
        offset = self._offset(offset)
        sql = f'SELECT {fields} FROM {table} {w_clause} {order} {limit} {offset}'
        return self.execute(sql, w_values)

    def _join_sql(self, tables, fields, join_fields, w_clause, order, limit, offset):
        assert len(tables) == len(fields) == len(join_fields)
        assert len(tables) == 2
        assert len(fields) == 2
        assert len(join_fields) == 2

        f_table1, f_table2 = fields
        f_table1 = [tables[0] + "." + f for f in f_table1]
        f_table2 = [tables[1] + "." + f for f in f_table2]
        fields =  ','.join(f_table1 + f_table2)

        sql = 'SELECT {0:s} FROM {1:s} LEFT JOIN {2:s} ON ({3:s} = {4:s})'.format(
            fields,
            tables[0],
            tables[1],
            '{0}.{1}'.format(tables[0], join_fields[0]),
            '{0}.{1}'.format(tables[1], join_fields[1]))

        order =self._order(order) 
        limit = self._limit(limit) 
        offset = self._offset(offset)

        sql += f'{w_clause} {order} {limit} {offset}'
        return sql

    def _join(self, tables, fields, join_fields, where=None, order=None, limit=None, offset=None):
        """Run an inner left join query"""
        w_clause, w_values = self._where(where)
        sql = self._join_sql(tables, fields, join_fields, w_clause, order, limit, offset)
        return self.execute(sql, w_values)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if not isinstance(exc_value, Exception):
            self._debug_write('Committing transaction')
            self.commit()
        else:
            self._debug_write('Rolling back transaction')
            self.rollback()

        self._cursor.close()

    def __del__(self):
        if self._connection:
            self._pool.put_conn(self._connection, fail_silently=True)
