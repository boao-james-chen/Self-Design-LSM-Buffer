import matplotlib.pyplot as plt


with open("./workload.log", "r") as f:
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

for key in stats.keys():
    plt.plot(stats[key], label=key)
plt.ylabel("op time (ms)")
plt.legend()
plt.show()
