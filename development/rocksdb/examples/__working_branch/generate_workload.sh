#!/bin/bash

# Function to display usage information
usage() {
  echo "Usage: $0 [options]"
  echo "Options:"
  echo "  -I <value>"
  echo "  -U <value>"
  echo "  -Q <value>"
  echo "  -R <value>"
  echo "  -S <value>"
  echo "  -D <value>"
  exit 1
}

# Set default values for all arguments
I_value=""
U_value=""
Q_value=""
R_value=""
S_value=""
D_value=""

# Parse command line arguments
while [[ $# -gt 0 ]]; do
  case "$1" in
    -I)
      I_value="$2"
      shift 2
      ;;
    -U)
      U_value="$2"
      shift 2
      ;;
    -Q)
      Q_value="$2"
      shift 2
      ;;
    -R)
      R_value="$2"
      shift 2
      ;;
    -S)
      S_value="$2"
      shift 2
      ;;
    -D)
      D_value="$2"
      shift 2
      ;;
    *)
      echo "Error: Unknown option '$1'"
      usage
      ;;
  esac
done

# Check if the required arguments are provided
#if [ -z "$I_value" ] || [ -z "$U_value" ]; then
#  echo "Error: The -I and -U arguments are required."
#  usage
#fi

# Call load_gen with the provided arguments
../K-V-Workload-Generator-master/load_gen -I "$I_value" -U "$U_value" -Q "$Q_value" -R "$R_value" -S "$S_value" -D "$D_value" 

echo "Workload generated successfully in workload.txt"
