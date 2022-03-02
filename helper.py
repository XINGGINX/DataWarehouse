import os
import json
import pandas as pd
import pymysql
import sqlalchemy


#=========================================================
#                   Load Configration 
#=========================================================


# Read configuration file: string -> dict
def read_config(path):
	try:
		conf = open(path)
		conf_dict = json.load(conf)
		conf.close()
		return conf_dict
	except:
		print("    read_config(): Fail read config from " + path)
		return False

	return False


def read_txt(path):
	try:
		# read sql file
		txt_file = open(path)
		return txt_file.read()
	except:
		print("    read_txt(): Fail read txt from " + path)
		return False

	return False


def get_dbname(index, conf):
	try:
		db_name = conf['controller']['database_option'][str(index)]
		return db_name
	except:
		print("    get_dbname(): Fail get database name.")
		return False

	return False


def switch_db(index, conf):
	try:
		new_index = index * -1
		print("    switch_db(): Switch database to " + get_dbname(new_index, conf))
		return new_index
	except:
		print("    switch_db(): Fail switch database")
		return index

	return new_index


def get_csv_files(path):
	try:
		files = os.listdir(path)
		csv_files = []

		for file in files:
			if ('csv' in file):
				csv_files.append(path + file)

		return csv_files
	except:
		print("    get_csv_files(): Fail get files from " + path)
		return False

	return False


#=========================================================
#                   MySQL helper function
#=========================================================

def drop_db(password, index, conf):
	try:
		db_name = get_dbname(index, conf)
		print("    drop_db(): Drop database '{}'.".format(get_dbname(index, conf)))


		cursor = create_connection(password, conf)
		drop_script = conf['mysql']['drop'].format(db_name)
		execute_sql_script(password, drop_script, conf)

	except Exception as error:
		print("    switch_db(): Fail drop database")
		print(str(error))


# Create connection with mysql using config file
def create_connection(password, conf):
	try:
		# parameters 
		host = conf['mysql']['local_connection']['host']
		user = conf['mysql']['local_connection']['user']

		# attempt establishing connection
		connection = pymysql.connect(host = host, user = user, password = password, autocommit = True)
		cursor = connection.cursor()
		return cursor

	except:
		print("    create_connection(): Fail connect with MySQL server.")
		return False

	return False



# Execute string type sql command 
def execute_sql_script(password, script, conf):
	try:
		# process big sql block
		print("    execute_sql_script(): Start execution.")
		cursor = create_connection(password, conf)
		script = script.replace('\n', ' ').replace('\t', ' ').split(';')

		# Execute each row of SQL script block
		n_exec = 0
		for cmd in script:
			if (len(cmd) >= 2):
				cursor.execute(cmd + ";")
				n_exec = n_exec + 1

	except Exception as error:
		# show error
		print("    execute_sql_script(): Fail execution ...")
		return False

	print("    execute_sql_script(): {}/{} commands successful executed.".format(n_exec, len(script)-1))

	return cursor



# landing data to sql
def csv_to_sql(password, db_name, conf, data, table_name, clash = 'append'):
	try:
		# Get engine conf and para
		step = "get para"
		host = conf['mysql']['local_connection']['host']
		user = conf['mysql']['local_connection']['user']
		port = conf['mysql']['local_connection']['port']

		step = "create engine"
		engine_script = conf['mysql']['engine']
		engine_script = engine_script.format(user = user, password = password, host = host, port = port, db_name = db_name)
		engine = sqlalchemy.create_engine(engine_script, echo = False)

		# CSV to SQL
		step = "import data"
		data.to_sql(name = table_name, con = engine, if_exists = clash, index = False)
		print("    csv_to_sql(): Success landing data to {}.{}.".format(db_name, table_name))
		return True

	except Exception as error:
		# log error
		print("    csv_to_sql(): Fail landing data to {}.{} during {}".format(db_name, table_name, step))
		return False

	return False


# landing data to sql
def csv_to_sql_one_line(password, db_name, conf, data, table_name, chunk = 0):
	try:
		# Get engine conf and para
		step = "get para"
		host = conf['mysql']['local_connection']['host']
		user = conf['mysql']['local_connection']['user']
		port = conf['mysql']['local_connection']['port']

		# Create engine 
		step = "create engine"
		engine_script = conf['mysql']['engine']
		engine_script = engine_script.format(user = user, password = password, host = host, port = port, db_name = db_name)
		engine = sqlalchemy.create_engine(engine_script, echo = False)

		# upload rows one by one 
		n_success = 0
		n = len(data)

		if (chunk == 0):
			for index, row in data.iterrows():
				record = pd.DataFrame(row).T
				try:
					record.to_sql(name = table_name, con = engine, if_exists = 'append', index = False)
					n_success = n_success + 1
				except:
					# fail into database for foreign key and duplicate row error
					pass
		else:
			chunk_size = int(data.shape[0] / chunk)
			for i in range(0, data.shape[0], chunk_size):
				data_subset = data.iloc[i:i + chunk_size]
				try:
					data_subset.to_sql(name = table_name, con = engine, if_exists = 'append', index = False)
					n_success = n_success + len(data_subset)
				except Exception as error:
					for index, row in data_subset.iterrows():
						record = pd.DataFrame(row).T
						try:
							record.to_sql(name = table_name, con = engine, if_exists = 'append', index = False)
							n_success = n_success + 1
						except:
							# fail into database for foreign key and duplicate row error
							pass

		print("    csv_to_sql_one_line(): Success landing {}/{} records to {}.{}.".format(n_success, n, table_name, db_name))

	except Exception as error:
		print("    csv_to_sql_one_line(): Fail landing data to {}.{} during {}".format(db_name, table_name, step))
		return False

	return False


def sql_to_csv(password, script, conf, table_name, db_name, delete_null = True, delete_duplicate = True): 
	try:
		# Get engine conf and para
		step = "get para"
		host = conf['mysql']['local_connection']['host']
		user = conf['mysql']['local_connection']['user']
		port = conf['mysql']['local_connection']['port']

		step = "create engine"
		engine_script = conf['mysql']['engine']
		engine_script = engine_script.format(user = user, password = password, host = host, port = port, db_name = db_name)
		engine = sqlalchemy.create_engine(engine_script, echo = False)

		# fetch data
		step = "fetch data"
		data = pd.read_sql(script, engine)
		n_rows = len(data)

		# pre clean data 
		step = "preclean"
		if (delete_null):
			data.dropna(inplace=True)
		if (delete_duplicate):
			data.drop_duplicates(inplace=True)

		print("    sql_to_csv(): Success select {}/{} records from {}.{}".format(len(data), n_rows, db_name, table_name))
		return data

	except Exception as error:
		# log error
		print("    sql_to_csv(): Fail select data from {}.{} during {}".format(db_name, table_name, step))
		return False

	return False


#=========================================================
#              sales_product table function
#=========================================================

# generate table dim_product
def generate_dim_product(product_list):
	product_list = product_list.drop_duplicates(keep='first', inplace = False)
	product_dict = pd.DataFrame({'product_name': product_list})

	return product_dict

# generate table dim_type
def generate_dim_type(type_list):
	type_list = type_list.drop_duplicates(keep='first', inplace = False).to_list()
	type_dict = pd.DataFrame({'type_name': type_list})

	return type_dict


# generate table dim_date
def generate_dim_date(date_series):
	try:
		# generate contiuous date strings 
		date_list = pd.date_range(start=min(date_series), end=max(date_series))

		# create pd dataframe
		date_code = []
		year = []
		month = []
		day = []
		quarter = []

		for date in date_list:
			date_code.append(str(date.date()))
			day.append(date.day)
			month.append(date.month)
			quarter.append(date.quarter)
			year.append(date.year)

		dim_date = pd.DataFrame({ 'date_code': date_code, "year": year, "month": month, 'day': day, "quarter": quarter})
		print("    generate_dim_date(): Success generate sql table -> dim_date")

		return dim_date

	except:
		print("    generate_dim_date(): Fail generate sql table -> dim_date")
		return False

	return False





















