# pip module
import os 
import time

# python module
import helper
import pymysql

# extract data from landing table
def extract_from_sql(password, db_index, conf):
	t1 = time.time()
	try:
		step = "get config"
		table_name = "landing"
		db_name = helper.get_dbname(db_index, conf)
		script = conf['mysql']['select'].format(db_name, table_name)
		print("  extract_from_sql(): Start extract data from {}.{}.".format(db_name, table_name))

		step = "to csv"
		data = helper.sql_to_csv(password, script, conf, "landing", db_name)

	except:
		print("  extract_from_sql(): Fail extract data from {}.{} during {}.".format(step, db_name, table_name))
		return False

	duration = str((int(time.time() - t1)) % 60)
	
	print("  extract_from_sql(): Duration = {} secounds.".format(duration))
	return data