#!/usr/bin/env python3
import pandas as pd
import uuid
import file_utils


# Returns the members patientId or creates a new one if it doesn't exist
patient_id_dict = {};
def get_patient_id(member_number):
    if member_number in patient_id_dict:
        return patient_id_dict[member_number]
    else:
        new_id = uuid.uuid4()
        patient_id_dict[member_number] = new_id
        return new_id
    
def create_medication_obj(row_data):
    quantity = row_data['CLM14_DECIMAL_QUANTITY_DISPENSED']
    days_supply = row_data['CLM15_DAYS_SUPPLY']
    units_per_day = quantity / days_supply if days_supply else 0
    return {
            'patientId': str(row_data['patientId']),
            'ndc9': row_data['ndc9'],
            'brandName': row_data['CLM11_PRODUCT_SERVICE_NAME'],
            'genericName': row_data['CLM12_PRODUCT_SERVICE_GENERIC_NAME'],
            'fillDate': str(row_data['CLM09_DATE_OF_SERVICE']),
            'quantity': row_data['CLM14_DECIMAL_QUANTITY_DISPENSED'],
            'daysSupply': row_data['CLM15_DAYS_SUPPLY'],
            'unitsPerDay': units_per_day,
            'dosageStrength': row_data['DRG14_STRENGTH'],
            'dosageUnit': row_data['CLM27_UNIT_DOSE_INDICATOR'],
            'doseForm': row_data['DRG25_DOSAGE_FORM'],
            'route': row_data['DRG13_ROUTE_DESCRIPTION']
            }

def process_data(filename, output_dir):
    # Read in relevant data from raw_pharmacy_1.csv and transform CLM09_DATE_OF_SERVICE to a datetime
    try:
        df = pd.read_csv(filename,
                         header=0,
                         usecols=['MBR01_MEMBER_NUMBER',
                                  'CLM10_PRODUCT_SERVICE_IDENTIFICATION',
                                  'CLM09_DATE_OF_SERVICE',
                                  'CLM11_PRODUCT_SERVICE_NAME',
                                  'CLM12_PRODUCT_SERVICE_GENERIC_NAME',
                                  'CLM09_DATE_OF_SERVICE',
                                  'CLM14_DECIMAL_QUANTITY_DISPENSED',
                                  'CLM15_DAYS_SUPPLY',
                                  'DRG14_STRENGTH',
                                  'CLM27_UNIT_DOSE_INDICATOR',
                                  'DRG25_DOSAGE_FORM',
                                  'DRG13_ROUTE_DESCRIPTION'],
                         parse_dates=['CLM09_DATE_OF_SERVICE'],
                         dtype={'CLM10_PRODUCT_SERVICE_IDENTIFICATION': str})
    except Exception:
        print("File Not Found")
        # Unsuccessful
        return False
        
    # Create a new column patientId with a new ID based on MBR01_MEMBER_NUMBER
    df['patientId'] = df['MBR01_MEMBER_NUMBER'].apply(get_patient_id)
    # Create a new column ndc9 that equals the substring of CLM10_PRODUCT_SERVICE_IDENTIFICATION
    df['ndc9'] = df['CLM10_PRODUCT_SERVICE_IDENTIFICATION'].str.slice(0, 9)
    # Clean NaNs in CLM14_DECIMAL_QUANTITY_DISPENSED, CLM15_DAYS_SUPPLY and DRG14_STRENGTH
    df['CLM14_DECIMAL_QUANTITY_DISPENSED'] = df['CLM14_DECIMAL_QUANTITY_DISPENSED'].fillna(0)
    df['CLM15_DAYS_SUPPLY'] = df['CLM15_DAYS_SUPPLY'].fillna(0)
    df['DRG14_STRENGTH'] = df['DRG14_STRENGTH'].fillna(0)
    
    # Group data by patientId
    patient_id_groups = df.groupby(['patientId'])
    for patient_id, patient_group in patient_id_groups:
        # Group individual patient results by ndc9 and CLM09_DATE_OF_SERVICE and aggregate data
        medicine_groups = patient_group.groupby(['patientId', 'ndc9', 'CLM09_DATE_OF_SERVICE']).agg({
                'CLM14_DECIMAL_QUANTITY_DISPENSED': 'sum',
                'CLM15_DAYS_SUPPLY': 'sum',
                'CLM11_PRODUCT_SERVICE_NAME': 'last',
                'CLM12_PRODUCT_SERVICE_GENERIC_NAME': 'last',
                'DRG14_STRENGTH': 'last',
                'CLM27_UNIT_DOSE_INDICATOR': 'last',
                'DRG25_DOSAGE_FORM': 'last',
                'DRG13_ROUTE_DESCRIPTION': 'last'
                }).reset_index()
        # Loop over aggregated data to create an array of json objects
        medication_objs = []
        for _, row_data in medicine_groups.iterrows():
            medication_objs.append(create_medication_obj(row_data))
        
        # Save the array of json objects to a file
        if medication_objs:
            file_utils.save_data_to_file(medication_objs, output_dir, patient_id, 'MedicationDispense')
    # Successful
    return True
            

#process_data('./raw_pharmacy_1.csv', './etl')    
    
    
