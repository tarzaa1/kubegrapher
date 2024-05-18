import csv
import random
import numpy as np
import time


def generate_events(num_users=20, sim_time=60, stay_prob=0.8):
    events = []
    zones = ['A', 'B', 'C']

    mall_layout = {
        'A': ['B'],
        'B': ['A', 'C'],
        'C': ['A'],
    }

    for user_idx in range(1, num_users + 1):
        user = f"user{user_idx}"
        arrival_time = random.randint(0, sim_time - 1)
        current_zone = random.choice(zones)

        # duration = int(np.random.normal(loc=mean_duration, scale=duration_std_dev))
        # last_action_time = min(arrival_time + duration, sim_time - 1)

        last_action_time = random.randint(arrival_time, sim_time - 1)

        events.append([arrival_time, 'Add', user, current_zone])
        user_last_zone = current_zone

        for t in range(arrival_time + 1, last_action_time + 1):
            if t == last_action_time:
                action = 'Delete' if random.random(
                ) < 0.5 else random.choice(['Move', 'Stay'])
            else:
                if random.random() < stay_prob:
                    action = 'Stay'
                else:
                    action = 'Move'

            if action == 'Move':
                new_zone = random.choice(mall_layout[user_last_zone])
                user_last_zone = new_zone
                events.append([t, 'Move', user, new_zone])
            elif action == 'Delete':
                events.append([t, 'Delete', user, user_last_zone])

        # events.sort(key=lambda x: x[0])

    return events


def save_events_to_csv(events, filename='user_events.csv'):
    with open(filename, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['Time', 'Action', 'Pod', 'Zone'])
        writer.writerows(events)


events = generate_events()
save_events_to_csv(events)
