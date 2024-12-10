import os
import subprocess
import shutil
import logging

# logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

# abs path to the project directory
project_dir = os.path.abspath(os.path.dirname(__file__))
logger.info(f"Project directory: {project_dir}")

# path to executables
load_gen_path = os.path.join(project_dir, 'bin', 'load_gen')
working_version_path = os.path.join(project_dir, 'bin', 'working_version')



buffer_implementations = {
#   1: 'skiplist',
#   2: 'vector',
#   3: 'hash_skip_list',
#   4: 'hash_linked_list',
  5: 'unsorted_vector',
  6: 'linklist' 
}



# -I: Inserts
# -U: Updates
# -Q: Point Queries
# -S: Range Queries
# -Y: Range Query Selectivity

workload_commands = [
    #insert
    # f"{load_gen_path} -I 10000 -U 0 -Q 0 -S 0 -Y 0.0",
    # f"{load_gen_path} -I 20000 -U 0 -Q 0 -S 0 -Y 0.0",
    # f"{load_gen_path} -I 30000 -U 0 -Q 0 -S 0 -Y 0.0",
    # f"{load_gen_path} -I 40000 -U 0 -Q 0 -S 0 -Y 0.0",
    #point query 
    f"{load_gen_path} -I 10000 -U 0 -Q 500 -S 0 -Y 0.0",
    # f"{load_gen_path} -I 10000 -U 0 -Q 1000 -S 0 -Y 0.0",
    # f"{load_gen_path} -I 10000 -U 0 -Q 1500 -S 0 -Y 0.0",
    #range query 
    # f"{load_gen_path} -I 10000 -U 0 -Q 0 -S 100 -Y 0.1",
    # f"{load_gen_path} -I 10000 -U 0 -Q 0 -S 100 -Y 0.2",
    # f"{load_gen_path} -I 10000 -U 0 -Q 0 -S 100 -Y 0.3",
    # f"{load_gen_path} -I 10000 -U 0 -Q 0 -S 100 -Y 0.4",

]

result_dir = os.path.join(project_dir, 'result')
os.makedirs(result_dir, exist_ok=True)

# delete the db folder after creating result folder
def delete_db_folder(db_folder_path):
    if os.path.exists(db_folder_path):
        try:
            shutil.rmtree(db_folder_path)
            logger.info(f"Deleted db folder at {db_folder_path}")
        except Exception as e:
            logger.error(f"Failed to delete db folder at {db_folder_path}: {e}")

#  remove trailing empty line 
def remove_trailing_newline(file_path):
    with open(file_path, 'rb+') as file:
        file.seek(-1, os.SEEK_END)
        last_char = file.read(1)

        # Check if the last character is a newline and remove it 
        if last_char == b'\n':
            file.seek(-1, os.SEEK_END)
            file.truncate()  # Removes the last newline character

    logger.info(f"Checked and removed trailing newline if present in {file_path}")

for workload_command in workload_commands:
    # get workload name for directory and log naming
    print(workload_command)
    workload_args = workload_command.replace(f"{load_gen_path} ", "")
    workload_name = workload_args.replace(" ", "-").replace("--", "-").replace("/", "-").replace("\\", "-")

    # using load_gen to generate workload
    logger.info(f"Generating workload '{workload_name}'")
    load_gen_cmd_list = workload_command.split()
    try:
        ret = subprocess.run(load_gen_cmd_list, cwd=project_dir, capture_output=True, text=True)
        logger.debug(f"load_gen output:\n{ret.stdout}\n{ret.stderr}")
    except Exception as e:
        logger.error(f"Something went wrong while running load_gen: {e}")
        continue 

    # check whether workload.txt exists
    workload_file = os.path.join(project_dir, 'workload.txt')
    if not os.path.exists(workload_file):
        logger.error(f"workload.txt does not exist after generating workload '{workload_name}'")
        continue

    # Remove trailing newline in workload.txt if exist
    remove_trailing_newline(workload_file)

    # create a nested folder in result section
    for impl_num, impl_name in buffer_implementations.items():
        logger.info(f"Running workload '{workload_name}' with buffer implementation '{impl_name}'")

        workload_result_dir = os.path.join(result_dir, impl_name, workload_name)
        os.makedirs(workload_result_dir, exist_ok=True)

        shutil.copy(workload_file, workload_result_dir)

        os.chdir(workload_result_dir)

        # delete db before running working_version
        db_folder_path = os.path.join(workload_result_dir, 'db')
        delete_db_folder(db_folder_path)

        # run working_version with the different buffer implementation (--memtable_factory option)
        working_command = [working_version_path, f"--memtable_factory={impl_num}", "-d1"]
        log_file_name = f"{workload_name}.log"
        with open(log_file_name, 'w') as log_file:
            try:
                ret = subprocess.run(working_command, stdout=log_file, stderr=subprocess.STDOUT)
                if ret.returncode != 0:
                    logger.error(f"Benchmark failed for workload '{workload_name}' with buffer implementation '{impl_name}'")
            except Exception as e:
                logger.error(f"Exception occurred while running working_version: {e}")
                continue 

        # after running everything, db should be in workload_result_dir
        delete_db_folder(db_folder_path)

        os.chdir(project_dir)

    os.remove(workload_file)

logger.info("Execution completed.")