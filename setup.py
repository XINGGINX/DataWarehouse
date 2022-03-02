# pip module
import os 
import time

# python module
import helper

# setup data warehouse
def setup_warehouse(password, db_index, conf):
	t1 = time.time()
	try:
		db_name = helper.get_dbname(db_index, conf)
		print("  setup_warehouse(): Start setup warehouse for {}.".format(db_name))
		
		ddl_path = conf['mysql']['schema'][db_name]['DDL']
		ddl_txt = helper.read_txt(ddl_path)

		helper.execute_sql_script(password, ddl_txt, conf)
		print("  setup_warehouse(): Success setup warehouse for {}.".format(db_name))

	except:
		print("  setup_warehouse(): Fail setup warehouse for {}.".format(db_name))
		return False

	duration = str((int(time.time() - t1)) % 60)
	print("  setup_warehouse(): Duration = {} secounds.".format(duration))
	return False
