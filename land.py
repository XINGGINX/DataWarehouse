# pip module
import os 
import time
import pandas as pd

# python module
import helper

# create and import landing table in sql
def land_to_warehouse(password, db_index, conf):
	t1 = time.time()

	try:
		# Get database name 
		step = "GET DB_NAME"

		table_name = "landing"
		db_name = helper.get_dbname(db_index, conf)

		# Read configuration 
		step = "GET CONFIG"

		csv_path = conf['csv'][db_name + "_raw_main"]
		csv_files = helper.get_csv_files(csv_path)

		# Transfer data to Mysql
		step = "TO SQL"

		current_file = 1
		n_files = len(csv_files)
		print("  land_to_warehouse(): Start landing {} csv file to {}.{}".format(n_files, db_name, table_name))

		for csv_file in csv_files:
			data = pd.read_csv(csv_file)

			# duplicate headers in sales data 
			if (db_index == 1):
				data = data.drop(data[data.Product == 'Product'].index)

			helper.csv_to_sql(password, db_name, conf, data, table_name)

			print("  land_to_warehouse(): progress = ({}/{})".format(current_file, n_files))
			current_file = current_file + 1

		print("  land_to_warehouse(): Success landing {} csv file to {}.{}".format(n_files, db_name, table_name))

	except Exception as error:
		print("  land_to_warehouse(): Fail landing csv file during '{}'.".format(step))
		print(str(error))
		return False


	duration = str((int(time.time() - t1)) % 60)
	print("  setup_warehouse(): Duration = {} secounds.".format(duration))

	return True
