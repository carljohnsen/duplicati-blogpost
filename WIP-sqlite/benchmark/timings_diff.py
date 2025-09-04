import datetime
import os

base_dir = 'lib/duplicati'
operations = ['backup', 'repair', 'restore', 'delete']
for op in operations:
    times = []
    for ver in ['120', '125']:
        with open(f'{base_dir}/{ver}-{op}.log') as f:
            begin, end = -1, -1
            time_format = '%H.%M.%S' if os.name == 'nt' else '%H:%M:%S'
            for line in f:
                if line.strip().lower().endswith(f'the operation {op} has started'):
                    begin = datetime.datetime.strptime(line.split(' ')[1], time_format)
                elif line.strip().lower().endswith(f'the operation {op} has completed'):
                    end = datetime.datetime.strptime(line.split(' ')[1], time_format)
            times.append((end - begin).total_seconds())
    print(f'{op} times: {times} seconds, diff: {times[1] - times[0]} seconds')
