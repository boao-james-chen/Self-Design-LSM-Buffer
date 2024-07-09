#!/bin/bash
sudo sh -c 'echo 0 >/proc/sys/kernel/perf_event_paranoid'
sudo sh -c 'echo 0 >/proc/sys/kernel/kptr_restrict'

if [ ! -d ~/.local/bin/FlameGraph ]; then
  mkdir -p ~/.local/bin/
  git clone https://github.com/brendangregg/FlameGraph.git ~/.local/bin/FlameGraph --depth=1
fi

rm -f ./out.perf ./out.folded ./flamegraph.svg
perf record -F 99 -a -g ./run.sh
perf script > out.perf
~/.local/bin/FlameGraph/stackcollapse-perf.pl out.perf > out.folded
~/.local/bin/FlameGraph/flamegraph.pl out.folded > flamegraph.svg
