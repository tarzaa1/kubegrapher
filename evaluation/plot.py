import pandas as pd
import matplotlib.pyplot as plt

def plot_csv_data(file_path, output_image_path):
    df = pd.read_csv(file_path)
    
    df['index'] = range(len(df))
    df.set_index('index', inplace=True)

    colors = ['b', 'g', 'r', 'c', 'm', 'y', 'k']
    
    plt.figure(figsize=(18, 8))
    
    for i, column in enumerate(df.columns.drop('event_id')):
        plt.plot(df.index, df[column], marker='o', color=colors[i % len(colors)], label=column)
    
    plt.title('Event Durations')
    plt.xlabel('Event ID')
    plt.ylabel('Duration (seconds)')
    plt.legend()
    
    plt.xticks(rotation=90)
    
    plt.tight_layout()
    
    plt.savefig(output_image_path)
    plt.close()

file_path = 'results/processed_records.csv'
output_image_path = 'results/plot.png'
plot_csv_data(file_path, output_image_path)