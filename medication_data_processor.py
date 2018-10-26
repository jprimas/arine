#!/usr/bin/env python3
import sys
import medication_processor as mp
import agg_medication_processor as amp

# Default values
data_file = './raw_pharmacy_1.csv'
output_dir = './etl'

if len(sys.argv) > 1 and sys.argv[1]:
    data_file = sys.argv[1]
if len(sys.argv) > 2 and sys.argv[2]:
    output_dir = sys.argv[2]

# Process the csv data file and create json files containing the patient's medication data
if mp.process_data(data_file, output_dir):
	# Process the json files to calculate aggregated data
	amp.process_json_files(output_dir)


