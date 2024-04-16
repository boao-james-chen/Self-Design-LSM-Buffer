/*
 *  Created on: May 13, 2019
 *  Author: Subhadeep
 */

#include "emu_environment.h"
#include "parse_arguments.h"
#include "run_workload.h"
#include "stats.h"

using namespace rocksdb;

int main(int argc, char* argv[]) {
  // check emu_environment.h for the contents of EmuEnv and also the definitions
  // of the singleton experimental environment
  EmuEnv* _env = EmuEnv::getInstance();
  // parse the command line arguments
  if (parse_arguments(argc, argv, _env)) {
    exit(1);
  }

  int s = runWorkload(_env);
  return 0;
}
