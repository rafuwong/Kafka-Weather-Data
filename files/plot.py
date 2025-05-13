import pandas as pd
import matplotlib.pyplot as plt
import os
import json


partition_files = []
monthly_dict = {}
for i in range(4): #Partition numbers are 0 to 3
    partition_file = f"/files/partition-{i}.json"
    if os.path.exists(partition_file):
        partition_files.append(partition_file)

for partition_file in partition_files:
    with open(partition_file, "r") as f:
        data = json.load(f)

    for month in data:
        if month in ['partition', 'offset']:
            continue
        if month in ["January", "February", "March"]:
            year_list = []
            for year in data[month]:
                year_list.append(int(year))
            year_list.sort(reverse = True) #Biggest year will be first element
            max_year = str(year_list[0])
            avg_temp = data[month][max_year]['avg']
            month_year_key = f"{month}-{max_year}"
            monthly_dict[month_year_key] = avg_temp

month_series = pd.Series(monthly_dict)

fig, ax = plt.subplots()
month_series.plot.bar(ax=ax)
ax.set_ylabel('Avg. Max Temperature')
plt.tight_layout()
plt.savefig("/files/month.svg")

