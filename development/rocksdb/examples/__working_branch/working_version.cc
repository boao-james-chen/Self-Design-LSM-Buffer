/*
 *  Created on: May 13, 2019
 *  Author: Subhadeep
 */

#include "parse_arguments.h"
#include "run_workload.h"
#include "emu_environment.h"

int main(int argc, char *argv[]) {
  EmuEnv* _env = EmuEnv::getInstance();
  Stats* fade_stats = Stats::getInstance();
  if (parse_arguments(argc, argv, _env)){
    exit(1);
  }

  if (_env->verbosity >= 4) {
    std::cout << "printing del_per_lat" << std::endl;
    for (int i = 0; i < 2; ++i){
      std::cout << i << ": " << EmuEnv::GetLevelDeletePersistenceLatency(i, _env) << std::endl;
    }
  }

  int s = runWorkload(_env); 

  fade_stats->printStats();

  return 0;
}
