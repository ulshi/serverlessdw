# coding = utf-8
from collections import defaultdict
from typing import List


class QueryDb:
    """
    Provide several methods to execute database query
    """
    def __init__(self):
        self.table = defaultdict(dict)
        self.tool = Dbtool()

    def create(self, table_name: str, col_names: List, keys: List):
        """create table"""
        if table_name not in self.table:
            self.table[table_name] = {'values': [], 'col_name': col_names, 'keys': keys}

    def insert(self, table_name: str, insert_values: List) -> None:
        table_values = self.table[table_name]['values']
        table_cols = self.table[table_name]['col_name']
        table_keys = self.table[table_name]['keys']
        for values in table_values:
            cur = 0
            tmp_keys = []
            for pos, val in enumerate(table_keys):
                if insert_values[pos] == values[pos]:
                    cur += 1
                    tmp_keys.append(val)
            if cur == len(table_keys):
                return
        self.table[table_name]['values'].append(insert_values)

    def select(self, table_name: str, conditions: List) -> List:
        table_values = self.table[table_name]['values']
        table_cols = self.table[table_name]['col_name']
        table_keys = self.table[table_name]['keys']
        res = []
        col_index = {}
        for index, col_name in enumerate(table_cols):
            col_index[col_name] = index
        for condition in conditions:
            col_name, col_val = self.tool.remove_space(condition, '=')
            if col_name not in table_cols:
                res = [col_name, 'Not exist']
                return res
        for condition in conditions:
            cur = 0
            tmp_res = []
            col_name, col_val = self.tool.remove_space(condition, '=')
            for value in table_values:
                if cur == 0:
                    if str(value[col_index[col_name]]) == str(col_val):
                        tmp_res.append(value)
                else:
                    if value in res and str(value[col_index[col_name]]) == str(col_val):
                        tmp_res.append(value)
            cur += 1
            res = tmp_res
        keys_index_list = [col_index[key] for key in table_keys]
        res = sorted(res, key=lambda x: [x[key_index] for key_index in keys_index_list])
        return res


class Dbtool:
    """
    Provide several Db tools
    """
    def __init__(self):
        pass

    def remove_space(self, simple, symbol=''):
        if symbol != '':
            return [i.strip() for i in simple.split(symbol)]
        return [i.strip() for i in simple.split()]



class Parse:
    """
    Provide several methods to parse SQL statement to command which QueryDb class need
    """
    def __init__(self):
        self.query = ''
        self.tool = Dbtool()

    def translate(self, query):
        self.query = query
        com, value = self.tool.remove_space(self.query.lower(), ':')
        # '''table_name, col_names, keys'''
        if com.strip() == 'create':
            table_name, col_names_str, keys_str = value.split(',')
            col_names = self.tool.remove_space(col_names_str)
            keys = self.tool.remove_space(keys_str)
            return [table_name, col_names, keys]
        # '''table_name, values'''
        if com.strip() == 'insert':
            table_name, values_str = value.split(',')
            values = self.tool.remove_space(values_str)
            return [table_name, values]
        # '''table_name, conditions'''
        if com.strip() == 'select':
            table_name, conditions_str = self.tool.remove_space(value, ',')
            conditions = self.tool.remove_space(conditions_str, 'and')
            return [table_name, conditions]


if __name__ == '__main__':
    num = 3
    parse = Parse()
    select = parse.translate('create: behavior, a, a')
    db = QueryDb()
    db.create(*select)
    insert = parse.translate('insert: behavior, 1')
    db.insert(*insert)
    db.insert(*insert)
    select = parse.translate('select: behavior, a=1')
    print(db.select(*select))
    print('success1')
    select = parse.translate('create: behavior_d, a b, a')
    db.create(*select)
    insert = parse.translate('insert: behavior_d, 1 5')
    db.insert(*insert)
    insert = parse.translate('insert: behavior_d, 1 2')
    db.insert(*insert)
    insert = parse.translate('insert: behavior_d, 1 3')
    db.insert(*insert)
    insert = parse.translate('insert: behavior_d, 2 3')
    db.insert(*insert)
    insert = parse.translate('insert: behavior_d, 23 5')
    db.insert(*insert)
    select = parse.translate('select: behavior_d, a=1')
    select = parse.translate('select: behavior_d, b=5')
    print(db.select(*select))
