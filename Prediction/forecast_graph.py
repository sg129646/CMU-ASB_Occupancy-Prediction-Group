import pandas as pd
import matplotlib.pyplot as plt

# 1. Load the forecast data
df = pd.read_csv('forecast.csv')
df['hour'] = pd.to_datetime(df['hour'])
df['hour_label'] = df['hour'].dt.strftime('%H:00')

rooms = sorted(df['room'].unique())
models = df['model'].unique()

# 2. Set up a 2x2 grid for the 4 rooms
fig, axes = plt.subplots(2, 2, figsize=(16, 10))
axes = axes.flatten()

# Colors and styles for consistent model lines
colors = {'RIDGE': 'tab:blue', 'XGB': 'tab:orange', 'TFT': 'tab:green', 
          'TimeLLM': 'tab:red', 'ENSEMBLE': 'black'}
linestyles = {'RIDGE': '--', 'XGB': '--', 'TFT': '-.', 
              'TimeLLM': ':', 'ENSEMBLE': '-'}

# 3. Plot each room
for i, room in enumerate(rooms):
    ax = axes[i]
    room_data = df[df['room'] == room]
    
    for model in models:
        model_data = room_data[room_data['model'] == model].sort_values('hour')
        
        # Emphasize the Ensemble model
        lw = 3.5 if model == 'ENSEMBLE' else 1.5
        alpha = 1.0 if model == 'ENSEMBLE' else 0.7
        
        ax.plot(model_data['hour_label'], model_data['prediction'], 
                label=model, color=colors.get(model, 'gray'), 
                linestyle=linestyles.get(model, '-'), 
                linewidth=lw, alpha=alpha, marker='o', markersize=4)
        
    ax.set_title(f"Room: {room.upper()}", fontsize=14, fontweight='bold')
    ax.grid(True, linestyle='--', alpha=0.5)
    ax.set_ylabel("Predicted Occupancy", fontsize=11)
    ax.tick_params(axis='x', rotation=45, labelsize=9)

# 4. Global formatting
axes[1].legend(title="Models", loc="upper right", bbox_to_anchor=(1.0, 1.0))
plt.suptitle("24-Hour Occupancy Forecast Comparison", fontsize=18, fontweight='bold', y=0.98)
plt.tight_layout()

# 5. Save the output
plt.savefig("all_models_comparison.png", dpi=300, bbox_inches='tight')
print("Plot successfully saved as 'all_models_comparison.png'")