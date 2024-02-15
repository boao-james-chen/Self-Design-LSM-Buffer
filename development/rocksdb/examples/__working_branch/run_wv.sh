#!/bin/bash

#variable = m (memtable_factory)
# Run the file with different values for -m and save output to separate files

#put the default values here


#other variables remain fixed
# for E in 64 128 256 1024; do #Entry size (B)
#   for value in 1 2 3 4; do
#     output_file="out_m_${value}_E_${E}.txt"
#     ./working_version -m "$value" -E "$E" > "./outputs/E_varied/$output_file"
#     echo "Run with -m $value completed. Output saved to $output_file"
#   done
# done

#other variables remain fixed
# for P in 1024 2048 4096; do # Buffer size in pages
#   for value in 1 2 3 4; do
#     output_file="out_m_${value}_P_${P}.txt"
#     ./working_version -m "$value" -P "$P" > "./outputs/P_varied/$output_file"
#     echo "Run with -m $value -P $P completed. Output saved to $output_file"
#   done
# done

# for B in 4 8 16 32; do #entries_per_page
#   for value in 1 2 3 4; do
#     output_file="out_m_${value}_B_${B}.txt"
#     ./working_version -m "$value" -B "$B" > "./outputs/B_varied/$output_file"
#     echo "Run with -m $value -B $B completed. Output saved to $output_file"
#   done
# done


# for T in 10 12; do #size_ratio
#   for value in 1 2 3 4; do
#     output_file="out_m_${value}_T_${T}.txt"
#     ./working_version -m "$value" -T "$T" > "./outputs/T_varied/$output_file"
#     echo "Run with -m $value -T $T completed. Output saved to $output_file"
#   done
# done

# for M in 8 16 32 64 128 256; do #Memory size (16 MB)
#  for M in 128 256; do #Memory size (16 MB)
#   for value in 1 2 3 4; do
#     output_file="out_m_${value}_M_${M}.txt"
#     ./working_version -m "$value" -M "$M" > "./outputs/M_varied/$output_file"
#     echo "Run with -m $value -M $M completed. Output saved to $output_file"
#   done
# done


# for F in 64 128 256; do #file size (KB)
#   for value in 1 2 3 4; do
#     output_file="out_m_${value}_F_${F}.txt"
#     ./working_version -m "$value" -F "$F" > "./outputs/F_varied/$output_file"
#     echo "Run with -m $value -F $F completed. Output saved to $output_file"
#   done
# done


# for C in 1 2 3 4; do #Compaction Style
#   for value in 1 2 3 4; do
#     output_file="out_m_${value}_C_${C}.txt"
#     ./working_version -m "$value" -C "$C" > "./outputs/C_varied/$output_file"
#     echo "Run with -m $value -C $C completed. Output saved to $output_file"
#   done
# done


# source_directory="outputs/T_varied"
# # Destination directory
# destination_directory="outputs/M_varied"

# # Move files starting with "out_m_" from source to destination
# for file in "$source_directory"/out_m_*_M_*; do
#     if [ -f "$file" ]; then
#         mv "$file" "$destination_directory/"
#         echo "Moved file: $file to $destination_directory/"
#     fi
# done


# for P in 8 16 32 64 128; do
#   for value in 1 2 3 4; do
#     output_file="out${value}.txt"
#     ./working_version -m "$value" > "./outputs/$output_file"
#     echo "Run with -m $value completed. Output saved to $output_file"
#   done
# done


for l in {1..30}; do # Prefix Length
  for value in 1 3 4; do
    output_file="out_m_${value}_l_${l}.txt"
    ./working_version -m "$value" -l "$l" > "./outputs/l_varied/$output_file"
    echo "Run with -m $value -l $l completed. Output saved to $output_file"
  done
done

#!/bin/bash

# Specify the base directory
# base_directory="/path/to/base/directory"

# # Create directories in a for loop
# for i in {1..5}; do
#     directory_name="directory_$i"
#     new_directory="$base_directory/$directory_name"
    
#     # Check if the directory already exists
#     if [ ! -d "$new_directory" ]; then
#         # Create the directory
#         mkdir "$new_directory"
#         echo "Created directory: $new_directory"
#     else
#         echo "Directory already exists: $new_directory"
#     fi
# done

