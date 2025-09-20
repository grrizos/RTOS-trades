import pandas as pd
import matplotlib.pyplot as plt


latency_file = "proof_log.csv"


df = pd.read_csv(latency_file)

# Μετατροπή timestamp από ns σε datetime
df['timestamp'] = pd.to_datetime(df['timestamp_ns'], unit='ns')
df.set_index('timestamp', inplace=True)

# Ομαδοποίηση ανά λεπτό 
df_minute = df.resample('1T').mean()

# Συνδυασμένο γράφημα με dual y-axis
fig, ax1 = plt.subplots(figsize=(12,6))

color = 'tab:blue'
ax1.set_xlabel('Time')
ax1.set_ylabel('Latency (us)', color=color)
ax1.plot(df_minute.index, df_minute['latency_us'], color=color, label='Latency (us)')
ax1.tick_params(axis='y', labelcolor=color)
ax1.axhline(0, color='gray', linestyle='--', linewidth=0.8)  # δείχνει το deadline

ax2 = ax1.twinx()  # δεύτερος άξονας Y
color = 'tab:green'
ax2.set_ylabel('CPU Idle (%)', color=color)
ax2.plot(df_minute.index, df_minute['cpu_idle_percent'], color=color, label='CPU Idle %')
ax2.tick_params(axis='y', labelcolor=color)


fig.tight_layout()
plt.title('Task Latency vs CPU Idle Time')
ax1.grid(True)

plt.savefig("latency_cpu_idle.png")
