import os
import re
import matplotlib.pyplot as plt

# LSM configuration info
ENTRY_SIZE = 64
ENTRIES_PER_PAGE = 2048
BUFFER_SIZE_IN_PAGES = 128
SIZE_RATIO = 4

buffer_implementations = {
   "skiplist": "Skiplist",
   "vector": "Vector",
   "hash_skip_list": "Hash Skip List",
   "hash_linked_list": "Hash Linked List",
   "unsorted_vector": "Unsorted Vector"
}

def extract_times(log_file):
   times = {}
   with open(log_file, "r") as file:
       for line in file:
           line = line.strip()
           if line.startswith("Total time taken by"):
               parts = line.split('=')
               if len(parts) == 2:
                   operation_part = parts[0].strip()
                   time_part = parts[1].strip()
                   operation = operation_part.replace("Total time taken by", "").strip()
                   time_ns = float(time_part.replace("ns", "").strip())
                   times[operation] = time_ns
   return times

# Get data from log files
data = {impl: {} for impl in buffer_implementations.keys()}
for impl in buffer_implementations.keys():
   impl_dir = os.path.join("result", impl)
   for workload_name in os.listdir(impl_dir):
       workload_dir = os.path.join(impl_dir, workload_name)
       if os.path.isdir(workload_dir):
           log_file_name = f"{workload_name}.log"
           log_path = os.path.join(workload_dir, log_file_name)
           if os.path.exists(log_path):
               times = extract_times(log_path)
               data[impl][workload_name] = times
           else:
               print(f"Log file not found: {log_path}")
       else:
           print(f"Not a directory: {workload_dir}")

operations = ["workload", "inserts", "queries", "updates", "range queries"]

all_workloads = set()
for impl_data in data.values():
   all_workloads.update(impl_data.keys())
sorted_workloads = sorted(all_workloads)

# Plot each operation's data with additional constant information
for operation in operations:
   plt.figure(figsize=(14, 10))  # Increased figure size
   for impl, impl_name in buffer_implementations.items():
       times = []
       for workload in sorted_workloads:
           time = data[impl].get(workload, {}).get(operation, None)
           if time is None:
               print(f"Time not found for operation '{operation}' in workload '{workload}' for implementation '{impl}'")
               time = 0
           times.append(time)
       plt.plot(sorted_workloads, times, label=impl_name)

   # Add constant information as text on the plot
   const_text = (f"ENTRY_SIZE = {ENTRY_SIZE} bytes\n"
                 f"ENTRIES_PER_PAGE = {ENTRIES_PER_PAGE}\n"
                 f"BUFFER_SIZE_IN_PAGES = {BUFFER_SIZE_IN_PAGES}\n"
                 f"SIZE_RATIO = {SIZE_RATIO}")
   plt.gcf().text(0.98, 0.02, const_text, fontsize=10, verticalalignment='bottom', ha='right',
                   bbox=dict(facecolor='white', alpha=0.5))  # Positioned at bottom-right corner

   plt.xlabel("Workload")
   plt.ylabel("Time (ns)")
   plt.title(f"Total time taken by {operation}")
   plt.legend(loc="upper left", bbox_to_anchor=(1.05, 1))  # Legend outside the plot area
   plt.xticks(rotation=60, ha="right")  # Increase rotation for clearer labels
   plt.tight_layout(pad=2)  # Add padding for better layout

   operation_name = operation.replace('/', '_')
   plt.savefig(f"result/{operation_name}_comparison.png", bbox_inches="tight")  # Save with tight bounding box
   plt.close()

print("Graphs saved in the 'result' directory.")
