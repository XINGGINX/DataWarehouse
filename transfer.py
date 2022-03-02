# pip module
import os 
import time
import pandas as pd

# python module
import helper


def transfer_accounting(password, conf, raw_data, db_index):
	try:
		# get db_name
		step = "config data"
		db_name = helper.get_dbname(db_index, conf)
		path = conf['csv']['accounting_raw_def']

		# remove duplicate raws
		raw_data = raw_data.drop_duplicates()

		# transfer dim_glcode
		step = "transfer dim_glcode"
		dim_glcode = pd.read_csv(path+"accounting_gl.csv")
		dim_glcode.columns = ['gl_code', 'desription']
		helper.csv_to_sql(password, db_name, conf, dim_glcode, "dim_glcode")


		# transfer fact_Hierarchy
		step = "transfer fact_Hierarchy"
		fact_Hierarchy = pd.read_csv(path+"accounting_hierarchy.csv")
		fact_Hierarchy.columns = ['node_id', 'node_name', 'parent_node']
		helper.csv_to_sql(password, db_name, conf, fact_Hierarchy, "fact_Hierarchy", clash = 'replace')


		# Table: dim_date -> pd.dataframe
		step = "transfer dim_date"

		datetime_list = pd.to_datetime(raw_data['PostingDate'])
		date_list = datetime_list.dt.date.to_list()
		dim_date = helper.generate_dim_date(date_list)
		helper.csv_to_sql(password, db_name, conf, dim_date, "dim_date")


		# Table: type -> pd.dataframe
		step = "transfer dim_type"

		type_list = raw_data['Type']
		dim_type = helper.generate_dim_type(type_list)
		helper.csv_to_sql(password, db_name, conf, dim_type, "dim_type")
		type_script = "SELECT * FROM {}.{};".format(db_name, "dim_type")
		type_dict = helper.sql_to_csv(password, type_script, conf, "dim_type", db_name)#['type_name'].to_dict()
		type_dict = type_dict.set_index('type_name').to_dict()['type_code']

		step = "transfer fact_accounting"
		gl_code = map(str, raw_data['GLCode'].to_list())
		date_code = map(str, raw_data['PostingDate'].to_list())
		type_code = map(int, raw_data['Type'].replace(type_dict))
		value = map(float, raw_data['Amount'].to_list())

		fact_accounting = pd.DataFrame({'gl_code': gl_code,
										'date_code': date_code,
										'type_code': type_code,
										'value': value
									})

		return fact_accounting

	except:
		print("  transfer_sales(): Fail transfer sales_product data during {}.".format(step))
		return False

	return fact_accounting


def transfer_sales(password, conf, raw_data, db_index):
	try:

		# get db_name
		step = "config data"
		db_name = helper.get_dbname(db_index, conf)

		# remove duplicate id for primary key 
		raw_data = raw_data.drop_duplicates(subset='Order ID', keep='first')

		# Table: dim_product -> pd.dataframe save product_dict
		step = "transfer dim_product"
		table_name = "dim_product"
		product_list = raw_data['Product']
		dim_product = helper.generate_dim_product(product_list)
		helper.csv_to_sql(password, db_name, conf, dim_product, table_name)
		product_script = "SELECT * FROM {}.{};".format(db_name, table_name)
		product_dict = helper.sql_to_csv(password, product_script, conf, table_name, db_name)#['product_name'].to_dict()
		product_dict = product_dict.set_index('product_name').to_dict()['product_code']


		# Table: dim_date -> pd.dataframe
		step = "transfer dim_date"

		datetime_list = pd.to_datetime(raw_data['Order Date'])
		date_list = datetime_list.dt.date.to_list()
		dim_date = helper.generate_dim_date(date_list)
		helper.csv_to_sql(password, db_name, conf, dim_date, "dim_date")

		# Table: fact_order -> pd.dataframe
		step = "transfer fact_order"

		order_id = raw_data['Order ID'].to_list()
		product_code = product_list.replace(product_dict)
		quantity = map(int, raw_data['Quantity Ordered'].to_list())
		item_price = map(float, raw_data['Price Each'].to_list())
		datetime_code = [str(date) for date in datetime_list.dt.date.to_list()]
		time = [str(time) for time in datetime_list.dt.time.to_list()]
		address = raw_data['Purchase Address'].to_list()

		fact_order = pd.DataFrame({ 'order_id': order_id,
									'product_code': product_code,
									'quantity': quantity,
									'item_price': item_price,
									'datetime_code': datetime_code,
									'time': time,
									'address': address
								})

		print("  transfer_sales(): Success transfer sales_product data")
		return fact_order

	except Exception as error:
		print("  transfer_sales(): Fail transfer sales_product data during {}.".format(step))

		return False

	return fact_order


# assign data to specified function
def transfer_data(password, conf, raw_data, db_index):
	t1 = time.time()
	print("  transfer_data(): Start transfer data")

	try:
		if db_index == -1:
			data_dict = transfer_accounting(password, conf, raw_data, db_index)
		else:
			data_dict = transfer_sales(password, conf, raw_data, db_index)

	except:
		print("  transfer_data(): Fail transfer data")
		return False

	duration = str((int(time.time() - t1)) % 60)
	print("  transfer_data(): Duration = {} secounds.".format(duration))

	return data_dict




