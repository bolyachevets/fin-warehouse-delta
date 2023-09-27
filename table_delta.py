import difflib
import os
from dataclasses import dataclass

@dataclass
class TableSchema:
    name: str
    cols: list

def process_table_delta(file_today, file_yesterday):
    hosts0 = open(file_today, "r")
    hosts1 = open(file_yesterday, "r")
    try:
        lines1 = hosts0.readlines()
        lines2 = hosts1.readlines()
        table = TableSchema('', [])
        sub1 = 'COPY'
        sub2 = 'FROM'

        for line in lines1:
            if sub1 in line:
                idx1 = line.index(sub1)
                idx2 = line.index(sub2)
                res = line[idx1 + len(sub1) + 1: idx2]
                els = res.split(' ')
                table.name = els[0]
                table.cols = els[1][1: len(els[1]) - 1].split(',')
                break

        with open(table.name + "_delta.sql", "w") as out:
            out.write('BEGIN;\n')
            out.write('SET client_encoding TO \'UTF8\';\n')
            out.write('SET synchronous_commit TO off;\n')

            diff = difflib.ndiff(lines2, lines1)
            delta = [l for l in diff if l.startswith('+') or l.startswith('-')]
            for line in delta:
                # print(line)
                if len(line) > 0:
                    line = line[:-1]
                    if line[0] == '-':
                        out.write('DELETE FROM ' + table.name + ' WHERE ')
                        vals = line[2:].split('\t')
                        for index, col in enumerate(table.cols):
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

            out.write('COMMIT;\n')

    finally:
        hosts0.close()
        hosts1.close()


if __name__ == '__main__':

    dir_today = os.environ['DIR_TODAY']
    dir_yesterday = os.environ['DIR_YESTERDAY']

    for i in os.listdir(dir_today):
        if os.path.isfile(os.path.join(dir_today,i)) and '_output.sql' in i:
            process_table_delta(os.path.join(dir_today,i), os.path.join(dir_yesterday,i))
