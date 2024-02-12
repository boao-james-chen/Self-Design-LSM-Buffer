#!/bin/bash

# Run the file with different values for -m and save output to separate files
for value in 1 2 3 4; do
  output_file="out${value}.txt"
  ./working_version -m "$value" > "./outputs/$output_file"
  echo "Run with -m $value completed. Output saved to $output_file"
done