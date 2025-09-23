import datetime
import os

base_dir = 'reports'
postfixes = ['_mac', '_t01', '_t02', '_win']
operations = ['backup', 'repair', 'restore', 'delete']
versions = ['120', '125']
times = {ver: { op: { 'begin': -1, 'end': -1 } for op in operations} for ver in versions}

for postfix in postfixes:
    time_format = None
    for ver in versions:
        with open(f'{base_dir}{postfix}/{ver}-summary.log') as f:
            for line in f:
                toks = line.strip().lower().split(' ')
                if toks[-2] != 'has' or (toks[-1] != 'started' and toks[-1] != 'completed'):
                    continue
                op = toks[-3]
                beginend = 'begin' if toks[-1] == 'started' else 'end'
                if time_format == None:
                    if ':' in toks[1]:
                        time_format = '%H:%M:%S'
                    else:
                        time_format = '%H.%M.%S'
                times[ver][op][beginend] = datetime.datetime.strptime(toks[1], time_format)

    print(f'Timings for {postfix}:')
    for op in operations:
        time_120 = (times['120'][op]['end'] - times['120'][op]['begin']).total_seconds()
        time_125 = (times['125'][op]['end'] - times['125'][op]['begin']).total_seconds()
        print(f'{op} times: [{time_120}, {time_125}] seconds, diff: {time_120 - time_125} seconds ({time_120 / time_125:.2f}x)')
    print()
