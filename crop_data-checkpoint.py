#Task 1a
# Usage: python crop_data.py dataset

import sys
import pandas as pd
import numpy as np

# Crop Dataset function
def crop_data(dataset):
	#Read in dataset with pandas and Set low_memory=False to stop DtypeWarning
	df = pd.read_csv(dataset, sep=';', low_memory=False, parse_dates=['Date Time'])

	#Convert "DateTime" columns to a pandas datetime object
	df['Date Time'] = pd.to_datetime(df['Date Time'])
	print(df.shape)
	#print(f"Size of DataFrame is {df.shape[0]} rows")

	#Crop file with records before 00:00 1 Jan 2010
	cropped_data = df.loc[df['Date Time'] > '2010-01-01 00:00:00+00:00']
        #print(f"Size of DataFrame is {cropped_data.shape[0]} rows")

	#Save file to a csv
	cropped_data.to_csv(f"{dataset.split('.')[0]}_cropped.csv", index=False)	

if __name__ == '__main__':
	dataset = sys.argv[1]
	crop_data(dataset)
