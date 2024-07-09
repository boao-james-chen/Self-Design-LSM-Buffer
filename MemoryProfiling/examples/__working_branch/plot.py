import sys
import matplotlib.pyplot as plt

def plot_workload(workload_path: str) -> None:
    with open(workload_path, "r") as f:
        workload_log = f.read()

    log, summary = workload_log.split("\n\n")

    stats = {
        "InsertTime": [0],
        "UpdateQueryTime": [0],
        "PointQueryTime": [0],
        "RangeQueryTime": [0],
    }

    for line in log.split("\n"):
        operation, ns = line.split(": ")
        stats[operation].append(int(ns))
        # stats[operation].append(int(ns) + stats[operation][-1])

    fig = plt.figure(num=f'Workload Plot: {workload_path}')
    fig.suptitle(f'Workload Plot: {workload_path}')

    axs = fig.subplots(len(stats))
    for key, ax in zip(stats.keys(), axs, strict=True):
        ax.set_title(key)
        ax.plot(stats[key], label=key)
        ax.legend()
        ax.set_ylabel("op time (ms)")

    plt.tight_layout()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        plot_workload("./workload.log")
    else:
        for workload_path in sys.argv[1:]:
            plot_workload(workload_path)

    plt.show()
