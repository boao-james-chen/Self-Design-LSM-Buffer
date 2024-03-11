## ============================================================ ##
##     RUN EXPERIMENTS AND ORGANISE RESULTS IN DIRECTOEIES      ##
## ============================================================ ##

import os
import sys
import shutil
from pathlib import Path

CWD = Path(os.getcwd())

force_make = False

if len(sys.argv) > 1:
    print(sys.argv)
    force_make = True

## ============================================================ ##
##                     SET EXPERIMENT DEFAULTS                  ##
## ============================================================ ##
## Also check arguments defined in __main__

# inserts = 190000
inserts = 140000  # 000
updates = 0
range_queries = 0 # 200
selectivity = 0 # 0.0001
point_queries = 0 # 200

# entry_sizes = [16, 32, 64, 128]
# entries_per_page = [256, 128, 64, 32]

entry_sizes = [64]
entries_per_page = [64]

# buffer_sizes_in_pages = [128, 512, 2048, 4096]
buffer_sizes_in_pages = [4096]

size_ratios = [2, 4, 6, 10]

SKIPLIST = "skiplist"
VECTOR = "vector"
HASHSKIPLIST = "hashskiplist"
HASHLINKLIST = "hashlinklist"

# refer into ./emu_environment.h
memtable_factories = {
    SKIPLIST: 1,
    VECTOR: 2,
    HASHSKIPLIST: 3,
    HASHLINKLIST: 4,
}

# these are only applicable in HashSkipList & HashLinkList
prefix_lengths = [0, 2, 6, 8, 10, 12]
bucket_counts = [1, 100, 1000, 5000, 8000, 10000]


## These are the default values for the experiments
prefix_lengths = [int(sys.argv[1])]
bucket_counts = [int(sys.argv[2])]


## ============================================================ ##
##                              END                             ##
## ============================================================ ##


def separator(func):
    def separator(*args, **kwargs):
        print("\n ------------------------------------------------------ ")
        func(*args, **kwargs)
        print(" ------------------------------------------------------ \n")

    return separator


def cleanup(dirpath: Path):
    workload_file = Path.joinpath(dirpath, "workload.txt")
    # db_working_home = Path.joinpath(dirpath, "db_working_home")
    # shutil.rmtree(db_working_home)
    workload_file.unlink()


def create_workload(args, dirpath: Path):
    os.chdir(dirpath.parent)
    load_gen = Path.joinpath(CWD.parent, "K-V-Workload-Generator-master", "load_gen")
    if not load_gen.exists() or force_make:
        os.chdir(load_gen.parent)
        print("Running make -j48 load_gen")
        os.popen("make -j48 load_gen").read()

    os.chdir(dirpath.parent)
    load_gen_cmd = f"../../K-V-Workload-Generator-master/load_gen -I {args['inserts']} -Q {args['point_queries']} -U {args['updates']} -S {args['range_queries']} -Y {args['selectivity']} -E {entry_size}"
    print(f"Running {load_gen_cmd}")
    os.popen(load_gen_cmd).read()
    print(f"Generated workload ...")


@separator
def run_worklaod(dirname, args, args_dict, exp_dir):

    dirpath = Path.joinpath(exp_dir, dirname)
    if not dirpath.exists():
        dirpath.mkdir()

    workload_file = Path.joinpath(dirpath.parent, "workload.txt")

    if not workload_file.exists():
        create_workload(
            args_dict,
            dirpath,
        )
    shutil.copy(workload_file, dirpath)

    if not Path.joinpath(dirpath, "workload.txt").exists():
        raise FileNotFoundError("workload.txt file NOT found!")

    working_version = Path.joinpath(CWD, "working_version")
    if not working_version.exists() or force_make:
        print("Running make -j48 working_version")
        os.popen("make -j48 working_version")

    working_version_cmd = f"{CWD}/working_version {args} --stat 1 > workload.log"
    os.chdir(dirpath)
    print(f"Running {working_version_cmd}")
    os.popen(working_version_cmd).read()
    print(f"Execution complete for {dirname}")
    cleanup(dirpath)


if __name__ == "__main__":
    size_ratios = [4]

    for entry_size, epp in zip(entry_sizes, entries_per_page):

        exp_dir = Path.joinpath(CWD, f"experiments-RQ-TEST4-{selectivity}-{entry_size}")

        if not exp_dir.exists():
            print(f"Creating new experiments directory: {exp_dir}")
            exp_dir.mkdir()

        print(f"Running experiments in director {exp_dir}")
        for memtable, moption in memtable_factories.items():
            for buffer_size_in_pages in buffer_sizes_in_pages:
                for size_ratio in size_ratios:
                    args_dict = {
                        "inserts": inserts,
                        "updates": updates,
                        "range_queries": range_queries,
                        "selectivity": selectivity,
                        "point_queries": point_queries,
                        "entry_size": entry_size,
                    }
                    dirname = f"I {inserts} U {updates} S {range_queries} Y {selectivity} Q {point_queries} m {memtable} E {entry_size} B {epp} P {buffer_size_in_pages} T {size_ratio}"
                    args = f"-m {moption} -E {entry_size} -B {epp} -P {buffer_size_in_pages} -T {size_ratio}"

                    if memtable in [SKIPLIST, VECTOR]:
                        run_worklaod(dirname, args, args_dict, exp_dir)
                    else:
                        for prefix_len in prefix_lengths:
                            for bucket_count in bucket_counts:
                                run_worklaod(
                                    f"{dirname} l {prefix_len} bucket_count {bucket_count}",
                                    f"{args} -l {prefix_len} --bucket_count {bucket_count} --threshold_use_skiplist {2 * inserts}",
                                    args_dict, exp_dir
                                )
