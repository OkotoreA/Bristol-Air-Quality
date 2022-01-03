#Task 1b
# Usage: python filter_data.py dataset

import sys
import pandas as pd
import numpy as np

#Filter Data Function
def filter_data(dataset):
	#Read in dataset with pandas and Set low_memory=False to stop DtypeWarning
	df = pd.read_csv(dataset, low_memory=False)

	#Make a copy of the dataframe
	new_df = df.copy()
	
	#Remove records where there is no value for SiteID
	#Remove SiteID 573.0, it has no location
	#Get unique site and location mapping and save as dictionary
	site_location_dict = new_df.loc[:, ['SiteID', 'Location']].drop_duplicates('SiteID', keep='first')[:-1].set_index('SiteID').to_dict()['Location']
	
	#mismatch between SiteID and Location
	for x in new_df.itertuples():
		try:
			loc = site_location_dict[x.SiteID]
			if (x.Location != loc):
				print(f'{x.Index} ---> {x.SiteID} ---> {x.Location}')
				new_df.drop(x.Index, inplace=True)
		except KeyError:
			continue
		finally:
			if x.SiteID == 573.0 or str(x.SiteID) == 'nan':
				print(f'{x.Index} ---> {x.SiteID} ---> {x.Location}')
				new_df.drop(x.Index, inplace=True)
	print(new_df.shape)
	new_df.to_csv(f"{dataset.split('.')[0]}_filter.csv", index=False)

if __name__ == '__main__':
	dataset = sys.argv[1]
	filter_data(dataset)
