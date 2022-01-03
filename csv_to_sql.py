#Task 3a
# Usage: python csv_to_sql.py dataset
# python csv_to_sql.py ../Task1/bristol-air-quality-data_cleaned.csv

import sys
import pandas as pd
import numpy as np

def generate_sql_file(dataset):
		#Read in dataset with pandas and Set low_memory=False to stop DtypeWarning
	df = pd.read_csv(dataset, low_memory=False)
	new_data = df.copy()
	
	new_data.loc[:, ['Date Time']] = new_data.loc[:, ['Date Time']].astype(str).applymap(lambda x: "'" + x + "'")
	new_data.loc[~new_data['DateStart'].isna(), ['DateStart']] = new_data.loc[~new_data['DateStart'].isna(), ['DateStart']].astype(str).applymap(lambda x: "'" + x + "'")
	new_data.loc[~new_data['DateEnd'].isna(), ['DateEnd']] = new_data.loc[~new_data['DateEnd'].isna(), ['DateEnd']].astype(str).applymap(lambda x: "'" + x + "'")

	new_data = new_data.fillna('NULL', inplace=False)
	site_id_dict = {}
	instrument_id_dict = {}
	date_dict = {}
	
	
	with open('bristol.sql', 'w') as file:
		file.write('INSERT INTO `location` (`location_id`, `site_id`, `location`, `long_lat`) VALUES')
		file.write('\n')
		data = new_data.loc[:, ['SiteID', 'Location', 'geo_point_2d']].drop_duplicates('SiteID', keep='first').reset_index()
		it = data.itertuples()
		for x in it:
			site_id_dict[x.SiteID] = x.Index+1
			file.write(f"""({x.Index+1}, {str(x.SiteID)}, "{str(x.Location)}", '{str(x.geo_point_2d)}')""")
			if x.Index != len(data)-1:
				file.write(',\n')
		file.write(';') 



		file.write('\n')    
		file.write('INSERT INTO `instrument` (`instrument_id`, `instrument_type`) VALUES')
		file.write('\n')
		data = new_data.loc[:, ['Instrument Type']].drop_duplicates('Instrument Type', keep='first').reset_index()
		it = data.itertuples()
		for x in it:
			instrument_id_dict[x._2] = x.Index+1
			file.write(f"({x.Index+1}, '{str(x._2)}')")
			if x.Index != len(data)-1:
				file.write(',\n')
		file.write(';') 

		file.write('\n')
		file.write('INSERT INTO `date` (`date_id`, `datestart`, `dateend`) VALUES')
		file.write('\n')
		data = new_data.loc[:, ['SiteID', 'DateStart', 'DateEnd']].drop_duplicates('SiteID', keep='first').reset_index()
		it = data.itertuples()
		for x in it:
			date_dict[x.DateStart] = x.Index+1
			file.write(f"({x.Index+1}, {str(x.DateStart)}, {x.DateEnd})")
			if x.Index != len(data)-1:
				file.write(',\n')
		file.write(';') 

		new_data['location_id'] = new_data['SiteID'].map(site_id_dict)
		new_data['instrument_id'] = new_data['Instrument Type'].map(instrument_id_dict)
		new_data['date_id'] = new_data['DateStart'].map(date_dict)


		file.write('\n')
		file.write('INSERT INTO `monitoring` (`monitoring_id`, `date_time`, `NOx`, `NO2`, `NO`, `PM10`, `NVPM10`, `VPM10`, `NVPM2.5`,`PM2.5`,`VPM2.5`, `CO`, `O3`, `SO2`,`Temperature`, `RH`, `Air_pressure`, `location_id`,  `instrument_id`, `date_id`) VALUES')
		file.write('\n')

		monitoring_columns = ['Date Time', 'NOx', 'NO2', 'NO', 'PM10', 'NVPM10', 'VPM10','NVPM2.5', 'PM2.5',
						  'VPM2.5', 'CO', 'O3', 'SO2', 'Temperature', 'RH','Air Pressure', 'location_id', 
						  'instrument_id','date_id']
		data = new_data.loc[:, monitoring_columns]
		it = data.itertuples()
		for x in it:
			file.write(f"""({x.Index+1}, {x._1}, {x.NOx}, {x.NO2}, {x.NO}, {x.PM10}, {x.NVPM10}, {x.VPM10}, {x._8}, {x._9}, {x._10}, {x.CO}, {x.O3}, {x.SO2}, {x.Temperature}, {x.RH}, {x._16}, {x.location_id}, {x.instrument_id}, {x.date_id})""")
			if x.Index != len(data)-1:
				file.write(',\n')
		file.write(';') 
    
	

if __name__ == '__main__':
	dataset = sys.argv[1]
	generate_sql_file(dataset)
