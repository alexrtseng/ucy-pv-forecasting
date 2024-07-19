import pandas as pd
import matplotlib.pyplot as plt
import xgboost as xgb

df = pd.read_csv('./data/UCY_SM_historical.csv', parse_dates=[0])

# Drop columns of SM not in use / messed up
drop_sm = ['Pac_Total-Meter-1', 'Pac_Total-Meter-2', 'time']
for i in range(20, 27):
    drop_sm.append(f'Pac_Total-Meter-{i}')

df = df.drop(columns=drop_sm)
df = df.dropna()
df = df[df['Total_Pac_MW'] <= 5]

# Create subplots
fig, axs = plt.subplots(3, 1, figsize=(15, 30))

# Plot 'Total_Pac_MW' and 'Pac_Total-Meter-{i}' columns for all i from 3 to 16
for i in range(3, 4):
    axs[i-3].plot(df['datetime'], df[f'Pac_Total-Meter-{i}'], label=f'Pac_Total-Meter-{i}')
    axs[i-3].set_xlabel('datetime')
    axs[i-3].set_ylabel('Power')
    axs[i-3].legend()
# for i in range(10, 17):
#     axs[i-3].plot(df['datetime'], df[f'Pac_Total-Meter-{i}'], label=f'Pac_Total-Meter-{i}')
#     axs[i-3].set_xlabel('datetime')
#     axs[i-3].set_ylabel('Power')
#     axs[i-3].legend()

axs[2].plot(df['datetime'], df['Total_Pac_MW'], label='Total_Pac_MW')

# Show the plot
plt.show()