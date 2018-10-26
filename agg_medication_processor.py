#!/usr/bin/env python3
import pandas as pd
import os
import file_utils

def create_medication_agg_obj(row_data):
    return {
            'patientId': str(row_data['patientId'].item()),
            'ndc9': row_data['ndc9']['last'],
            'brandName': row_data['brandName']['last'],
            'genericName': str(row_data['genericName'].item()),
            'quantity': row_data['quantity']['last'],
            'daysSupply': row_data['daysSupply']['last'],
            'unitsPerDay': row_data['unitsPerDay']['last'],
            'dosageStrength': row_data['dosageStrength']['last'],
            'dosageUnit': row_data['dosageUnit']['last'],
            'doseForm': row_data['doseForm']['last'],
            'route': row_data['route']['last'],
            'firstFillDate': str(row_data['fillDate']['first']),
            'lastFillDate': str(row_data['fillDate']['last']),
            'cumDaysSupply': row_data['daysSupply']['sum']
            }

def process_patient(output_dir, filename):
    # Read in a single json file
    df = pd.read_json(filename, orient='records')
    #convert fillDate to DateTime
    df['fillDate'] = pd.to_datetime(df['fillDate'])
    #sort by fillDate
    sorted_df = df.sort_values(by='fillDate')
    agg_df = sorted_df.groupby(['patientId', 'genericName']).agg({
            'ndc9':'last',
            'brandName':'last',
            'quantity':'last',
            'daysSupply':['last','sum'],
            'unitsPerDay':'last',
            'dosageStrength':'last',
            'dosageUnit':'last',
            'doseForm':'last',
            'route':'last',
            'fillDate':['first', 'last']
            }).reset_index()
    med_agg_objs = []
    for _, row_data in agg_df.iterrows():
        med_agg_objs.append(create_medication_agg_obj(row_data))
    if med_agg_objs:
        file_utils.save_data_to_file(med_agg_objs, output_dir, df['patientId'][0], 'MedicationEra')
    
    
# Read and process each MedicationDispense.json file
def process_json_files(rootdir):
    for subdir, _, files in os.walk(rootdir):
        for file in files:
            if file == 'MedicationDispense.json':
                process_patient(rootdir, os.path.join(subdir, file))

#process_json_files('./etl')
            
            
