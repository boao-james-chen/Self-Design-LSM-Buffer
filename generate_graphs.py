import os
import re
import matplotlib.pyplot as plt
import numpy as np
import statistics

# LSM configuration info
ENTRY_SIZE = 64
ENTRIES_PER_PAGE = 2048
BUFFER_SIZE_IN_PAGES = 128
SIZE_RATIO = 4

buffer_implementations = {
    "unsorted_vector": "Unsorted Vector",
    "linklist": "Linked List"
}


expected_labels = {
    "InsertTime",
    "PointQueryTime",
    "RangeQueryTime",
    "SnapshotCreationTime(GetIterator)",
    "SortingTime"
}

def extract_per_operation_times(log_file):
    per_operation_times = {}
    total_snapshot_creation_time = 0
    total_sorting_time = 0
    with open(log_file, "r") as file:
        for line in file:
            line = line.strip()
            # Use regular expression to match timing entries
            match = re.match(r'^(.*?):\s*([\d\.e+-]+)\s*(ns)?$', line)
            if match:
                op_time_name = match.group(1).strip()
                time_str = match.group(2).strip()
                unit = match.group(3)
                if op_time_name in expected_labels:
                    try:
                        time_ns = float(time_str)
                        if unit == 'ns' or unit is None:
                            pass  # time_ns is already in nanoseconds
                        else:
                            print(f"Unknown unit '{unit}' in line: '{line}'")
                            continue
                    except ValueError:
                        print(f"Could not convert time value '{time_str}' to float in line: '{line}'")
                        continue

                    if op_time_name == "SnapshotCreationTime(GetIterator)":
                        total_snapshot_creation_time += time_ns
                    elif op_time_name == "SortingTime":
                        total_sorting_time += time_ns
                    else:
                        if op_time_name not in per_operation_times:
                            per_operation_times[op_time_name] = []
                        per_operation_times[op_time_name].append(time_ns)
            else:
                # Line does not match expected format; skip it
                continue
    return per_operation_times, total_snapshot_creation_time, total_sorting_time

# Extract per-operation times from logs
data = {impl: {} for impl in buffer_implementations.keys()}
total_times = {impl: {} for impl in buffer_implementations.keys()}
for impl in buffer_implementations.keys():
    impl_dir = os.path.join("result", impl)
    for workload_name in os.listdir(impl_dir):
        workload_dir = os.path.join(impl_dir, workload_name)
        if os.path.isdir(workload_dir):
            log_file_name = f"{workload_name}.log"
            log_path = os.path.join(workload_dir, log_file_name)
            if os.path.exists(log_path):
                per_op_times, total_snapshot_time, total_sort_time = extract_per_operation_times(log_path)
                data[impl][workload_name] = per_op_times
                total_times[impl][workload_name] = {
                    'total_snapshot_time': total_snapshot_time,
                    'total_sorting_time': total_sort_time
                }
            else:
                print(f"log file does not exist at: {log_path}")
        else:
            print(f"Not a directory at: {workload_dir}")

# mapping log entry names to db op types
operation_types = {
    'Insert': 'InsertTime',
    'Point Query': 'PointQueryTime',  
    'Range Query': 'RangeQueryTime',
}

# Initialize mean latencies data structure
mean_latencies = {impl: {} for impl in buffer_implementations.keys()}

# Collect per-operation times and compute mean latencies
for impl in buffer_implementations.keys():
    for op_name, log_entry_name in operation_types.items():
        all_times = []
        for workload_name, per_op_times in data[impl].items():
            times = per_op_times.get(log_entry_name, [])
            all_times.extend(times)
        if all_times:
            mean_latency = sum(all_times) / len(all_times)
            mean_latencies[impl][op_name] = mean_latency
            print(f"{impl} - {op_name}: Collected {len(all_times)} times, mean latency = {mean_latency}")
        else:
            mean_latencies[impl][op_name] = None
            print(f"{impl} - {op_name}: No data collected")

    # Sum up total snapshot creation and sorting times across all workloads
    total_snapshot_time = sum(
        workload_times['total_snapshot_time'] for workload_times in total_times[impl].values()
    )
    total_sorting_time = sum(
        workload_times['total_sorting_time'] for workload_times in total_times[impl].values()
    )
    print(f"{impl} - Total Snapshot Creation Time: {total_snapshot_time} ns")
    print(f"{impl} - Total Sorting Time: {total_sorting_time} ns")

operations = list(operation_types.keys())
buffer_impls = list(buffer_implementations.keys())

n_groups = len(operations)
n_bars = len(buffer_impls)

plt.figure(figsize=(10, 6))

index = np.arange(n_groups)
bar_width = 0.8 / n_bars

for i, impl in enumerate(buffer_impls):
    mean_latency_list = []
    for op in operations:
        avg_latency = mean_latencies[impl].get(op)
        if avg_latency is not None:
            mean_latency_list.append(avg_latency)
        else:
            mean_latency_list.append(0)
    bars = plt.bar(index + i * bar_width, mean_latency_list, bar_width, label=buffer_implementations[impl])

    # Add text labels on top of each bar
    for bar in bars:
        height = bar.get_height()
        if height > 0:
            plt.text(
                bar.get_x() + bar.get_width() / 2,
                height,
                f'{height:.2f}',
                ha='center',
                va='bottom',
                fontsize=8
            )

# lsm configuration info
const_text = (f"ENTRY_SIZE = {ENTRY_SIZE} bytes\n"
              f"ENTRIES_PER_PAGE = {ENTRIES_PER_PAGE}\n"
              f"BUFFER_SIZE_IN_PAGES = {BUFFER_SIZE_IN_PAGES}\n"
              f"SIZE_RATIO = {SIZE_RATIO}")
plt.gcf().text(0.98, 0.02, const_text, fontsize=5, verticalalignment='bottom', ha='right',
                bbox=dict(facecolor='white', alpha=0.5))

plt.xlabel('Operation Type')
plt.ylabel('Mean Latency (ns)')
plt.title('Mean Query Latency by Operation and Buffer Implementation')
plt.xticks(index + bar_width * (n_bars - 1) / 2, operations)
plt.legend()
plt.tight_layout()
plt.savefig('result/mean_query_latency.png')
plt.close()

print("Graph saved in the 'result' directory.")