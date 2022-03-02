import helper
import time


def load_data(password, conf, data, db_index):
	print("  load_data(): Start load data")
	t1 = time.time()

	try:
		# get db_name and table_name
		step = "config data"
		db_name = helper.get_dbname(db_index, conf)
		table_name = conf['mysql']['schema'][db_name]['main']
		if (db_index == -1):
			helper.csv_to_sql_one_line(password, db_name, conf, data, table_name)
		else:
			helper.csv_to_sql_one_line(password, db_name, conf, data, table_name, chunk = 5)
	except Exception as error:
		print("  load_data(): Fail load data to {} during '{}'.".format(db_name, step))

	duration = str((int(time.time() - t1)) % 60)
	print("  setup_warehouse(): Duration = {} secounds.".format(duration))

	return False