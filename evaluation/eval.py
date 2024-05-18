import csv
from dateutil import parser

input_file = "results/records.csv"
output_file = "results/processed_records.csv"

with open(input_file, newline="\n") as csvFile:
    reader = csv.reader(csvFile)

    with open(output_file, mode='w', newline='') as outFile:
        writer = csv.writer(outFile)
        writer.writerow(['event_id', 'end_to_end', 'processing_time', 'latency', 'consensus_latency'])
        
        for row in reader:
            event_id = row[0]
            event_sent_at = parser.isoparse(row[1])
            event_received_at = parser.isoparse(row[2])
            event_processed_at = parser.isoparse(row[3])
            event_reached_consensus_at = parser.isoparse(row[4])
            
            end_to_end = event_processed_at - event_sent_at
            processing_time = event_processed_at - event_received_at
            latency = event_received_at - event_sent_at
            consensus_latency = event_reached_consensus_at - event_sent_at
            
            writer.writerow([event_id, end_to_end.total_seconds(), processing_time.total_seconds(), latency.total_seconds(), consensus_latency.total_seconds()])

