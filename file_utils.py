#!/usr/bin/env python3
import os
import json

# Write data to a file with the given filename
def save_data_to_file(data, output_dir, patient_id, filename):
    filename = output_dir + '/' + str(patient_id) + '/' + filename + '.json'
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename, 'w') as outfile:
        json.dump(data, outfile)
