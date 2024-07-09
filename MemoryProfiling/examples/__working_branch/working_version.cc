/*
 *  Created on: May 13, 2019
 *  Author: Subhadeep
 */

#include "db_env.h"
#include "parse_arguments.h"
#include "run_workload.h"
#include "stats.h"

using namespace rocksdb;

int main(int argc, char* argv[]) {
  DBEnv* env = DBEnv::GetInstance();

  if (parse_arguments(argc, argv, env)) {
    exit(1);
  }

  runWorkload(env);
  return 0;
}
