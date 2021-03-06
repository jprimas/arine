# Arine
ETL Take Home Assignment For Arine using Pandas

My solution's output can be found in the subdirectory `./etl/`

### How to Run Solution using Pandas
After cloning the repository navigate to the main directory and run:
```shell
python medication_data_processor.py
```
Optionally, you can specify the datafile and output directory as optional arguments
```shell
python medication_data_processor.py <datafile> <output_directory>
```
These arguments normally default to `./raw_pharmacy_1.csv` and `./etl` respectively

### Code Layout for Solution using Pandas
#### medication_data_processor.py
This is the main method to call initially that handles input arguments and calls into medication_processor.py and agg_medication_processor.py when appropriate
#### medication_processor.py
Implements the first 5 steps of the assignment: reads in the CSV file, transforms it accordingly, aggregates over patientId, ndc9, and fillDate, and creates a json for each patient with the aggregated information
#### agg_medication_processor.py
Implements steps 6 through 8 of the assignment: reads in the newly created json files from above for each patient, aggregates over patientId and genericName, and creates a new json with the aggregated information for each patient
#### file_utils.py
This is a helper class containing shared methods between `medication_processor.py` and `agg_medication_processor.py`


### Solution using AWS Glue + Spark
Under the folder `spark` I have put my two scripts that use AWS Glue and PySpark to complete the assignment.
The two scripts have similar functionalities to their counterparts using pandas except they save the output as parquets partitioned over the patientId instead of json files.
The files are called `aws_medication_processor.py` and `aws_agg_medication_processor.py`
