# pip module
import os 
import time

# python module
import helper
import setup
import land
import extract
import transfer
import load


# configuration JSON file
config = "./config/conf.json"

# section boarder 
section_start_log = "-" * 90
section_finish_log = section_start_log + "\n"

#=========================================================
#                  Controller
#=========================================================

def controller(password):
	try:
		# read configuration
		conf = helper.read_config(config)
		controller_conf = conf["controller"]

		# database options - default accounting
		db_option = controller_conf['database_option']
		db_index = -1
		print("controller(): Current database = " + db_option[str(db_index)])

		accounting_raw_data = False
		sales_raw_data = False
		accounting_transfered = False
		sales_transfered = False

	except Exception as error:
		print("controller(): Fail read config file")
		return False

	while True:
		command = input("controller(): Enter command: ").lower()

		# switch database 
		if (command in controller_conf["switch_db"]):
			print(section_start_log)

			db_index = helper.switch_db(db_index, conf)

			print(section_finish_log)


		# setup data warehouse
		elif (command in controller_conf["setup"]):
			print(section_start_log)

			setup.setup_warehouse(password, db_index, conf)

			print(section_finish_log)


		# land data to warehouse
		elif (command in controller_conf["landing"]):
			print(section_start_log)

			land.land_to_warehouse(password, db_index, conf)

			print(section_finish_log)


		# extract raw_data from sql 
		elif (command in controller_conf["extract"]):
			print(section_start_log)

			if (db_index == -1):
				accounting_raw_data = extract.extract_from_sql(password, db_index, conf)
			else:
				sales_raw_data = extract.extract_from_sql(password, db_index, conf)

			print(section_finish_log)


		# transfer main data and upload dim_tables
		elif (command in controller_conf["transfer"]):
			print(section_start_log)

			if (db_index == -1):
				if (type(accounting_raw_data) == type(False)):
					print("controller(): Can not parse accounting data to transfer_data()")
				else:
					accounting_transfered = transfer.transfer_data(password, conf, accounting_raw_data, db_index)
			elif (db_index == 1):
				if (type(sales_raw_data) == type(False)):
					print("controller(): Can not parse sales_product data to transfer_data()")
				else:
					sales_transfered = transfer.transfer_data(password, conf, sales_raw_data, db_index)
			else:
				print("controller(): Unexpected error occured.")

			print(section_finish_log)


		# load main data to sql
		elif (command in controller_conf["load"]):
			print(section_start_log)

			if (db_index == -1):
				if (type(accounting_transfered) == type(False)):
					print("controller(): Can not parse accounting data to load_data()")
				else:
					load.load_data(password, conf, accounting_transfered, db_index)
			elif (db_index == 1):
				if (type(sales_transfered) == type(False)):
					print("controller(): Can not parse sales_product data to load_data()")
				else:
					load.load_data(password, conf, sales_transfered, db_index)
			else:
				print("controller(): Unexpected error occured.")

			print(section_finish_log)


		# AUTO
		elif (command in controller_conf["auto"]):
			print(section_start_log)

			setup.setup_warehouse(password, db_index, conf)
			land.land_to_warehouse(password, db_index, conf)

			if (db_index == -1):
				accounting_raw_data = extract.extract_from_sql(password, db_index, conf)
			else:
				sales_raw_data = extract.extract_from_sql(password, db_index, conf)

			if (db_index == -1):
				if (type(accounting_raw_data) == type(False)):
					print("controller(): Can not parse accounting data to transfer_data()")
				else:
					accounting_transfered = transfer.transfer_data(password, conf, accounting_raw_data, db_index)
			elif (db_index == 1):
				if (type(sales_raw_data) == type(False)):
					print("controller(): Can not parse sales_product data to transfer_data()")
				else:
					sales_transfered = transfer.transfer_data(password, conf, sales_raw_data, db_index)
			else:
				print("controller(): Unexpected error occured.")

			if (db_index == -1):
				if (type(accounting_transfered) == type(False)):
					print("controller(): Can not parse accounting data to load_data()")
				else:
					load.load_data(password, conf, accounting_transfered, db_index)
			elif (db_index == 1):
				if (type(sales_transfered) == type(False)):
					print("controller(): Can not parse sales_product data to load_data()")
				else:
					load.load_data(password, conf, sales_transfered, db_index)
			else:
				print("controller(): Unexpected error occured.")

			print(section_finish_log)



		# Drop database 
		elif (command in controller_conf["drop"]):
			print(section_start_log)

			helper.drop_db(password, db_index, conf)

			print(section_finish_log)


		# Exit 
		elif (command in controller_conf["exit"]):
			print("controller(): Shut down ... ")
			time.sleep(0.5)
			print(40*"\n")
			break


		# Safe Net
		else:
			print("controller(): Command unrecognised ... \n")






controller("199881")
