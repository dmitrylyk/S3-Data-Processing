import random
import json

USERS = ['A', 'B', 'C', 'D']
EVENTS = ['tutorial', 'iap']


def generate_timestamp():
    day = random.randint(1, 30)
    hour = random.randint(0, 23)
    minute = random.randint(0, 59)
    second = random.randint(0, 59)

    return f'2020-11-{day} {hour}:{minute}:{second}'


dicts = []

for _ in range(random.randint(1, 10)):
    new_dict = {'user': random.choice(USERS), 'timestamp': generate_timestamp()}

    evt = random.choice(EVENTS)
    if evt == 'iap':
        new_dict['spend'] = round(random.uniform(0.1, 20), 2)

    new_dict['evtname'] = evt

    dicts.append(new_dict)

with open("FILENAME", "w") as write_file:
    json.dump(dicts, write_file)
