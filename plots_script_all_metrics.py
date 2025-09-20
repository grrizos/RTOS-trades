import pandas as pd
import matplotlib.pyplot as plt

# Load metrics
df = pd.read_csv("all_metrics.csv", parse_dates=["timestamp"])

symbols = df["symbol"].unique()

for sym in symbols:
    sym_df = df[df["symbol"] == sym]


    plt.figure(figsize=(12,5))
    plt.plot(sym_df["timestamp"], sym_df["moving_avg"], marker='o', linestyle='-')
    plt.title(f"{sym} 15-min Moving Average")
    plt.xlabel("Time")
    plt.ylabel("Price")
    plt.xticks(rotation=45)
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(f"{sym}_moving_avg.png", dpi=300)
    plt.close() 

  
    plt.figure(figsize=(12,5))
    plt.plot(sym_df["timestamp"], sym_df["volume_sum"], marker='o', linestyle='-', color='orange')
    plt.title(f"{sym} 15-min Volume Sum")
    plt.xlabel("Time")
    plt.ylabel("Volume")
    plt.xticks(rotation=45)
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(f"{sym}_volume_sum.png", dpi=300)
    plt.close()
