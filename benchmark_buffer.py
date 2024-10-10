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
    1: 'skiplist',
    # 2: 'vector',
    # 3: 'hash_skip_list',
    # 4: 'hash_linked_list',
    # 5: 'unsorted_vector'
}

# -I: Inserts
# -U: Updates
# -Q: Point Queries
# -S: Range Queries
# -Y: Range Query Selectivity

workload_commands = [
    # f"{load_gen_path} -I 100000 -U 0 -Q 0 -S 0 -Y 0.0",      
    # f"{load_gen_path} -I 100000 -U 1000 -Q 0 -S 0 -Y 0.0",  
    # f"{load_gen_path} -I 100000 -U 500 -Q 250 -S 0 -Y 0.0", 
    # f"{load_gen_path} -I 100000 -U 0 -Q 500 -S 0 -Y 0.0",   
    # f"{load_gen_path} -I 100000 -U 500 -Q 450 -S 0 -Y 0.0", 
    # f"{load_gen_path} -I 100000 -U 1000 -Q 900 -S 0 -Y 0.0", 
    # f"{load_gen_path} -I 100000 -U 5000 -Q 500 -S 0 -Y 0.0", 
    # f"{load_gen_path} -I 100000 -U 1000 -Q 750 -S 0 -Y 0.0", 
    # f"{load_gen_path} -I 100000 -U 500 -Q 995 -S 0 -Y 0.0",  
    # f"{load_gen_path} -I 100000 -U 0 -Q 1000 -S 0 -Y 0.0",   
    
    #range query
    f"{load_gen_path} -I 100000 -U 10 -Q 0 -S 1 -Y 0.001", 
    # f"{load_gen_path} -I 100000 -U 10 -Q 0 -S 100 -Y 0.2",  
    # f"{load_gen_path} -I 100000 -U 10 -Q 0 -S 150 -Y 0.3",   
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

for workload_command in workload_commands:
    # get workload name for directory and log naming
    workload_args = workload_command.replace(f"{load_gen_path} ", "")
    workload_name = workload_args.replace(" ", "-").replace("--", "-").replace("/", "-").replace("\\", "-")

    #  using load_gen to generate workload
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

    # create a nested folder in result section
    for impl_num, impl_name in buffer_implementations.items():
        logger.info(f"Running workload '{workload_name}' with buffer implementation '{impl_name}'")

        workload_result_dir = os.path.join(result_dir, impl_name, workload_name)
        os.makedirs(workload_result_dir, exist_ok=True)

        shutil.copy(workload_file, workload_result_dir)

        os.chdir(workload_result_dir)

        # Delete the 'db' folder before running working_version
        db_folder_path = os.path.join(workload_result_dir, 'db')
        delete_db_folder(db_folder_path)

        # Run working_version with the different buffer implementation (--memtable_factory option)
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

        # after running everything, the 'db' folder should be in workload_result_dir
        # Now delete the 'db' folder to save memory
        delete_db_folder(db_folder_path)

        os.chdir(project_dir)

    os.remove(workload_file)

logger.info("Script execution completed.")