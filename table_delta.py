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
    junk = ['+ \n', '- \n', 'COPY', 'None', '\\.']
    for j in junk:
        if j in line:
            return True
    return False


def process_chunk(out, b1, b2, table):
    diff = difflib.ndiff(b2, b1)
    delta = []
    for l in diff:
        if l.startswith('+') or l.startswith('-'):
            delta.append(l)
    for line in delta:
        vals = line[2:].split('\t')
        if '' in vals:
            continue
        if len(line) > 0 and not isJunk(line):
            line = line[:-1]
            if line[0] == '-':
                out.write('DELETE FROM ' + table.name + ' WHERE ')
                vals = line[2:].split('\t')
                for index, col in enumerate(vals):
                    if vals[index] == '\\N':
                        out.write(col + ' is NULL')
                    else:
                        out.write(col + '=\'' + vals[index] + '\'')
                    if index < len(table.cols) - 1:
                        out.write(' and ')
                    else:
                        out.write(';\n')
            elif line[0] == '+':
                out.write('INSERT INTO ' + table.name + ' (')
                for index, col in enumerate(table.cols):
                    out.write(col)
                    if index < len(table.cols) - 1:
                        out.write(',')
                out.write(') VALUES (')
                vals = line[2:].split('\t')
                for index, v in enumerate(vals):
                    if vals[index] == '\\N':
                        out.write('NULL')
                    else:
                        out.write('\'' + v + '\'')
                    if index < len(table.cols) - 1:
                        out.write(',')
                    else:
                        out.write(');\n')


def process_table_delta(file_today, file_yesterday):
    dir_today = os.environ['DIR_TODAY']

    hosts0 = open(file_today, "r")
    hosts1 = open(file_yesterday, "r")
    try:
        table = TableSchema('', [])
        sub1 = 'COPY'
        sub2 = 'FROM STDIN'
        with open(dir_today + '/' + '_delta.sql', "w") as out:
            out.write('BEGIN;\n')
            out.write('SET client_encoding TO \'UTF8\';\n')
            out.write('SET synchronous_commit TO off;\n')
            b1 = []
            b2 = []
            for n, lines in enumerate(itertools.zip_longest(hosts0, hosts1)):
                if sub1 in lines[0] and sub2 in lines[0]:
                    idx1 = lines[0].index(sub1)
                    idx2 = lines[0].index(sub2)
                    res = lines[0][idx1 + len(sub1) + 1: idx2]
                    els = res.split(' ')
                    table.name = els[0]
                    table.cols = els[1][1: len(els[1]) - 1].split(',')
                b1.append(lines[0])
                b2.append(lines[1])
                if n > 0 and n % 100000 == 0:
                    b2 = ['' if v is None else v for v in b2]
                    process_chunk(out, b1, b2, table)
                    b1 = []
                    b2 = []
            b2 = ['' if v is None else v for v in b2]
            process_chunk(out, b1, b2, table)
            out.write('COMMIT;\n')
            os.rename(dir_today + '/' + '_delta.sql', dir_today + '/' + table.name + '_delta.sql')
    finally:
        hosts0.close()
        hosts1.close()


if __name__ == '__main__':

    dir_today = os.environ['DIR_TODAY']
    dir_yesterday = os.environ['DIR_YESTERDAY']
    clean_dirs = int(os.getenv('CLEAN_DIRS', 0))

    os.remove(os.path.join(dir_today, 'output.sql'))

    for i in os.listdir(dir_today):
        if os.path.isfile(os.path.join(dir_today,i)) and '_output.sql' in i:
            process_table_delta(os.path.join(dir_today,i), os.path.join(dir_yesterday,i))
            if clean_dirs == 1:
                os.remove(os.path.join(dir_yesterday,i))
                shutil.move(os.path.join(dir_today,i), os.path.join(dir_yesterday,i))
