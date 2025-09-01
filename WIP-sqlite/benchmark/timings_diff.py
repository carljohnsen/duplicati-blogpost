import datetime

base_dir = 'lib/duplicati'
operations = ['backup', 'repair', 'restore', 'delete']
for op in operations:
    times = []
    for ver in ['120', '125']:
        with open(f'{base_dir}/{ver}-{op}.log') as f:
            begin, end = -1, -1
            for line in f:
                if line.strip().lower().endswith(f'the operation {op} has started'):
                    begin = datetime.datetime.strptime(line.split(' ')[1], '%H:%M:%S')
                elif line.strip().lower().endswith(f'the operation {op} has completed'):
                    end = datetime.datetime.strptime(line.split(' ')[1], '%H:%M:%S')
            times.append((end - begin).total_seconds())
    print(f'{op} times: {times} seconds, diff: {times[1] - times[0]} seconds')
