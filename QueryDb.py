# coding = utf-8
from collections import defaultdict
from typing import List

class QueryDb:
    def __init__(self):
        self.table = defaultdict(dict)


    def create(self, table_name: str, col_num: int, col_names: List, keys: List):
        """create table"""
        if table_name not in self.table:
            self.table[table_name] = {'values': [], 'col_num': col_num, 'col_name': col_names, 'keys': keys}


    def insert(self, table_name: str, values: List) -> None:
        table_keys = self.table[table_name]['keys']
        keys_index_list = [1, 2]
        table_values = self.table[table_name]['values']
        table_cols = self.table[table_name]['col_name']
        table_keys = self.table[table_name]['keys']
        for values in table_values:
            cur = 0
            tmp_keys = []
            for pos, val in enumerate(table_keys):
                if table_cols[pos] == values[pos]:
                    cur += 1
                    tmp_keys.append(val)
            if cur == len(table_keys):
                return tmp_keys
        self.table[table_name]['values'].append(values)


    def select(self, table_name: str, conditions: List) -> List:
        table_values = self.table[table_name]['values']
        table_cols = self.table[table_name]['col_name']
        table_keys = self.table[table_name]['keys']
        res = []
        col_index = {}
        for index, col_name in enumerate(table_cols):
            col_index[col_name] = index
        for condition in conditions:
            col_name, col_val = map(str, condition.split('=').stripe())
            if col_name not in table_cols:
                res = [col_name, 'Not exist']
                return res
        for condition in conditions:
            cur = 0
            tmp_res = []
            col_name, col_val = map(str, condition.split('=').stripe())
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


class Parse:
    def __init__(self):
        self.query = ''


    def translate(self, query):
        self.query = query
        com, value = self.query.lower().split(':')
        # '''table_name, col_num, col_names, keys'''
        if com.strip() == 'create':
            table_name, col_num, col_names_str, keys_str = value.split(',')
            col_names = [i.strip() for i in col_names_str.split(',')]
            keys = [i.strip() for i in keys_str.split(',')]
            return [table_name, int(col_num), col_names, keys]
        # '''table_name, values'''
        if com.strip() == 'insert':
            table_name, values_str = value.split(',')
            values = [i.strip() for i in values_str.split(' ')]
            return [table_name, values]
        # '''table_name, conditions'''
        if com.strip() == 'select':
            table_name, conditions_str = value.split(',')
            conditions = [i.strip() for i in conditions_str.split('and')]
            return [table_name, conditions]




if __name__ == '__main__':
        parse = Parse()
        select = parse.translate('create: behavior, 1, a, a')
        db = QueryDb()
        db.create(*select)
        insert = parse.translate('insert: behavior, 1')
        db.insert(*insert)
        db.insert(*insert)
        select = parse.translate('select: behavior, a=1')
        db.select(*select)
        print('success')
