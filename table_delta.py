import difflib
import os
import shutil
import itertools
from dataclasses import dataclass

@dataclass
class TableSchema:
    name: str
    cols: list


def isJunk(line):
    junk = ['+ \\.\n', '- \\.\n', 'search_path', 'FROM STDIN']
    if line is None:
        return True
    for j in junk:
        if j in line:
            return True
    return False


def preprocess_chunk(b1, b2, diff_dict):
    diff = difflib.ndiff(b2, b1)
    delta = []
    for l in diff:
        if l.startswith('+') or l.startswith('-'):
            delta.append(l)
    for line in delta:
        if len(line) > 0 and not isJunk(line) and not line == '\\t':
            vals = line[2:-1]
            if vals in diff_dict:
                diff_dict.pop(vals, None)
            else:
                if line[0] == '-':
                    diff_dict[vals] = '-'
                elif line[0] == '+':
                    diff_dict[vals] = '+'


def write_diff_to_file(out, table, diff_dict):
    for val, op in diff_dict.items():
        if op == '-':
            if len(val) > 0 and not isJunk(val) and not val == '\\t':
                val_split = val.split('\t')
                if '' in val_split:
                    continue
                out.write('DELETE FROM ' + table.name + ' WHERE ')
                for index, col in enumerate(table.cols):
                    out.write(col)
                    if val_split[index] == '\\N':
                        out.write(' is NULL')
                    else:
                        v = val_split[index]
                        if '\'' in v:
                            v = v.replace('\'', '\'\'')
                        out.write('=E\'' + v + '\'')
                    if index < len(table.cols) - 1:
                        out.write(' and ')
                    else:
                        out.write(';\n')
    for val, op in diff_dict.items():
        if op[0] == '+':
            if len(val) > 0 and not isJunk(val) and not val == '\\t':
                val_split = val.split('\t')
                if '' in val_split:
                    continue
                out.write('INSERT INTO ' + table.name + ' (')
                for index, col in enumerate(table.cols):
                    out.write(col)
                    if index < len(table.cols) - 1:
                        out.write(',')
                out.write(') VALUES (')
                for index, v in enumerate(val_split):
                    if v == '\\N':
                        out.write('NULL')
                    else:
                        if '\'' in v:
                            v = v.replace('\'', '\'\'')
                        out.write('E\'' + v + '\'')
                    if index < len(table.cols) - 1:
                        out.write(',')
                    else:
                        out.write(');\n')


def process_table_delta(file_today, file_yesterday):
    dir_today = os.environ['DIR_TODAY']

    hosts0 = open(file_today, "r")
    hosts1 = open(file_yesterday, "r")

    diff_dict = {}
    try:
        schema_loaded = False
        table = TableSchema('', [])
        sub1 = 'COPY'
        sub2 = 'FROM STDIN'
        with open(dir_today + '/' + '_delta.sql', "w") as out:
            out.write('BEGIN;\n')
            out.write('SET client_encoding TO \'UTF8\';\n')
            out.write('SET synchronous_commit TO off;\n')
            b1 = []
            b2 = []
            print('diffing file chunks...')
            for n, lines in enumerate(itertools.zip_longest(hosts0, hosts1)):
                if not schema_loaded and sub1 in lines[0] and sub2 in lines[0]:
                    idx1 = lines[0].index(sub1)
                    idx2 = lines[0].index(sub2)
                    res = lines[0][idx1 + len(sub1) + 1: idx2]
                    els = res.split(' ')
                    table.name = els[0]
                    table.cols = els[1][1: len(els[1]) - 1].split(',')
                    schema_loaded = True
                if not isJunk(lines[0]):
                    b1.append(lines[0])
                if not isJunk(lines[1]):
                    b2.append(lines[1])
                if n > 0 and n % chunk_size == 0:
                    b2 = ['' if v is None else v for v in b2]
                    print(n)
                    preprocess_chunk(b1, b2, diff_dict)
                    b1 = []
                    b2 = []
                    print('moving to next chunk...')
            b2 = ['' if v is None else v for v in b2]
            print('diffing remainder...')
            preprocess_chunk(b1, b2, diff_dict)
            print('preparing to write to file...')
            write_diff_to_file(out, table, diff_dict)
            out.write('COMMIT;\n')
            os.rename(dir_today + '/' + '_delta.sql', dir_today + '/' + table.name + '_delta.sql')
            print('delta ready...')
    finally:
        hosts0.close()
        hosts1.close()


if __name__ == '__main__':

    dir_today = os.environ['DIR_TODAY']
    dir_yesterday = os.environ['DIR_YESTERDAY']
    clean_dirs = int(os.getenv('CLEAN_DIRS', 0))
    chunk_size = int(os.getenv('CHUNK_SIZE', 100000))

    for i in os.listdir(dir_today):
        if os.path.isfile(os.path.join(dir_today,i)) and '_output.sql' in i:
            print('starting...')
            print(i)
            process_table_delta(os.path.join(dir_today,i), os.path.join(dir_yesterday,i))
            if clean_dirs == 1:
                os.remove(os.path.join(dir_yesterday,i))
                shutil.move(os.path.join(dir_today,i), os.path.join(dir_yesterday,i))
            print('processed...')
