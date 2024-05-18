import csv
import time
import kubeActions
from pprint import pprint

def run_simulation(filename='user_events.csv', sim_time=60, namespace="cikm"):
    events_dict = {}
    with open(filename, mode='r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            t = int(row['Time'])
            event = {
                'Action': row['Action'],
                'Pod': row['Pod'],
                'Zone': row['Zone']
            }
            if t not in events_dict:
                events_dict[t] = []
            events_dict[t].append(event)
    
    for time_step in range(sim_time):
        if time_step in events_dict:
            for event in events_dict[time_step]:
                action = event['Action']
                pod = event['Pod']
                zone = event['Zone']
                if action == 'Add':
                    kubeActions.add_pod(pod, zone, namespace)
                elif action == 'Delete':
                    kubeActions.delete_pod(pod, namespace)
                elif action == 'Move':
                    kubeActions.move_pod(pod, zone, namespace)
            time.sleep(5)
    
    kubeActions.add_pod("done", 'A', namespace)
    time.sleep(5)
    kubeActions.force_delete_all_pods(namespace)

run_simulation()
