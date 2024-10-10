import os
import re
import matplotlib.pyplot as plt


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
            # grep "Total time taken by workload = 14027028 ns"
            if line.startswith("Total time taken by"):
                parts = line.split('=')
                if len(parts) == 2:
                    operation_part = parts[0].strip()
                    time_part = parts[1].strip()
                    operation = operation_part.replace("Total time taken by", "").strip()
                    time_ns = float(time_part.replace("ns", "").strip())
                    times[operation] = time_ns
    return times

# get data from log files
data = {impl: {} for impl in buffer_implementations.keys()}
for impl in buffer_implementations.keys():
    impl_dir = os.path.join("result", impl)
    for workload_name in os.listdir(impl_dir):
        workload_dir = os.path.join(impl_dir, workload_name)
        if os.path.isdir(workload_dir):
            #  path to the log file
            log_file_name = f"{workload_name}.log"
            log_path = os.path.join(workload_dir, log_file_name)
            if os.path.exists(log_path):
                times = extract_times(log_path)
                data[impl][workload_name] = times
            else:
                print(f"Log file not found: {log_path}")
        else:
            print(f"Not a directory: {workload_dir}")


operations = ["workload", "inserts", "queries", "updates"]


all_workloads = set()
for impl_data in data.values():
    all_workloads.update(impl_data.keys())
sorted_workloads = sorted(all_workloads)


for operation in operations:
    plt.figure()
    for impl, impl_name in buffer_implementations.items():
        times = []
        for workload in sorted_workloads:
            # Get the time for the current operation, default to None if missing
            time = data[impl].get(workload, {}).get(operation, None)
            if time is None:
                # if the time is missing set it to 0
                print(f"Time not found for operation '{operation}' in workload '{workload}' for implementation '{impl}'")
                time = 0
            times.append(time)
        plt.plot(sorted_workloads, times, label=impl_name)
    
    plt.xlabel("Workload")
    plt.ylabel("Time (ns)")
    plt.title(f"Total time taken by {operation}")
    plt.legend()
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    operation_name = operation.replace('/', '_')
    plt.savefig(f"result/{operation_name}_comparison.png")
    plt.close()

print("graphps saved in the 'result' directory.")
