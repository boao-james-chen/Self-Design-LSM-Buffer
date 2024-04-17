#!/bin/bash

# This script sets up a project repository by updating submodules and building necessary components.
# It uses color coding and selective output for clarity.

set -e  # Exit immediately if a command exits with a non-zero status.

# Color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

function main() {
    echo -e "${YELLOW}Starting setup process...${NC}"
    update_submodules
    build_load_gen
    build_static_lib
    build_working_branch
    echo -e "\n${GREEN}====> Setup complete! <====${NC}"
}

function update_submodules() {
    echo -e "\n${YELLOW}[Step 1/4] Updating submodules...${NC}"
    git submodule update --init --recursive | grep "Entering" || true
    git submodule foreach git pull origin main | grep "Already up to date" || true
    echo -e "${GREEN}Submodules updated.${NC}"
}

function build_load_gen() {
    echo -e "\n${YELLOW}[Step 2/4] Building load_gen component...${NC}"
    if [ -d "KV-WorkloadGenerator" ]; then
        cd KV-WorkloadGenerator
        make_output=$(make load_gen)
        echo "${make_output}" | grep "is up to date" || echo -e "${GREEN}load_gen built successfully.${NC}"
        cd ..
    else
        echo -e "${RED}Error: KV-WorkloadGenerator directory does not exist.${NC}"
        exit 1
    fi
}

function build_static_lib() {
    echo -e "\n${YELLOW}[Step 3/4] Building static_lib using all available cores...${NC}"
    if [ -d "MemoryProfiling" ]; then
        cd MemoryProfiling
        make_output=$(make static_lib -j$(nproc))
        echo "${make_output}" | grep "Nothing to be done for 'static_lib'" || echo -e "${GREEN}static_lib built successfully.${NC}"
        cd ..
    else
        echo -e "${RED}Error: MemoryProfiling directory does not exist.${NC}"
        exit 1
    fi
}

function build_working_branch() {
    echo -e "\n${YELLOW}[Step 4/4] Building working_branch...${NC}"
    if [ -d "MemoryProfiling/examples/__working_branch" ]; then
        cd MemoryProfiling/examples/__working_branch
        make_output=$(make working_version 2>&1)
        echo "${make_output}" | grep "warning" || echo -e "${GREEN}working_branch built successfully.${NC}"
        cd ..
    else
        echo -e "${RED}Error: MemoryProfiling/examples/__working_branch directory does not exist.${NC}"
        exit 1
    fi
}

# Start the main script
main
