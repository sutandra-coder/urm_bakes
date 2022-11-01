from flask import Flask, request, jsonify, json
from flask_api import status
from jinja2._compat import izip
from datetime import datetime,timedelta,date
import pymysql
from flask_cors import CORS, cross_origin
from flask import Blueprint
from flask_restplus import Api, Resource, fields
from werkzeug.datastructures import FileStorage
from werkzeug.utils import secure_filename
import requests
import calendar
import json
from threading import Thread
import time
import os   

app = Flask(__name__)
cors = CORS(app)

bakes_and_cakes_admin = Blueprint('bakes_and_cakes_admin_api', __name__)
api = Api(bakes_and_cakes_admin,  title='BakesAndCakesAdmin API',description='Bakes And Cakes Admin API')
name_space = api.namespace('BakesAndCakesAdmin',description='BakesAndCakesAdmin')

name_space_salesman_dashboard = api.namespace('BakesAndCakeSalesManDashboard',description='BakesAndCakeSalesManDashboard')


#----------------------database-connection---------------------#

def bakesandcakes():
	connection = pymysql.connect(host='recess.cdcuaa7mp0jm.us-east-2.rds.amazonaws.com',
									user='recess_admin',
									password='hiHFy1FFJ9L1VDjmDo11',
									db='bakesandcakes',
									charset='utf8mb4',
								cursorclass=pymysql.cursors.DictCursor)
	return connection


def mysql_connection():
	connection = pymysql.connect(host='creamsonservices.com',
	                             user='creamson_langlab',
	                             password='Langlab@123',
	                             db='bakesandcakes',
	                             charset='utf8mb4',
	                             cursorclass=pymysql.cursors.DictCursor)
	return connection

#----------------------database-connection---------------------#

product_inventory_post_model = api.model('product_inventory_post_model',{
	"product_id":fields.Integer(required=True),
	"qty":fields.Integer(required=True),	
	"last_update_id":fields.Integer(required=True),
	"inventory_id":fields.Integer(required=True)
})

product_model = api.model('task_model ', {
	"product_id":fields.Integer(),
	"qty": fields.Integer()
})

sell_product_post_model = api.model('sell_product_post_model',{
	"retailer_id":fields.Integer(required=True),
	"sales_amount":fields.String(required=True),
	"products": fields.List(fields.Nested(product_model )),
	"salesman_id":fields.Integer(required=True)
})

full_filled_post_model = api.model('full_filled_post_model',{
	"sales_order_id":fields.Integer(required=True),
	"inventory_id":fields.Integer(required=True),
	"salesman_id":fields.Integer(required=True)
})

full_filled_and_partial_fullfill_post_model =  api.model('full_filled_and_partial_fullfill_post_model',{
	"is_fullfill":fields.Integer(required=True),
	"sales_order_id":fields.Integer(required=True),
	"inventory_id":fields.Integer(required=True),
	"products": fields.List(fields.Nested(product_model )),
	"salesman_id":fields.Integer(required=True)
})

is_paid_post_model = api.model('is_paid_post_model',{
	"sales_order_id":fields.Integer(required=True),
	"payment_type":fields.Integer(required=True),
	"transaction_id":fields.String(required=True),
	"salesman_id":fields.Integer(required=True)
})

product_post_model = api.model('product_post_model',{
	"product_name":fields.String(required=True),
	"product_type_id":fields.Integer(required=True),
	"in_price":fields.Integer(required=True),
	"out_price":fields.Integer(required=True),
	"last_update_id":fields.Integer(required=True)
})

salesman_post_model = api.model('salesman_post_model',{
	"name":fields.String(required=True),
	"phoneno":fields.String(required=True),
	"password":fields.String(required=True),
	"secondary_number":fields.String(required=True),
	"admin_id":fields.Integer(required=True)
})

payment_post_model = api.model('payment_post_model',{
	"payment_type":fields.Integer(required=True),
	"payment_amount":fields.String(required=True),
	"payment_mode":fields.String(required=True),
	"retailer_id":fields.Integer(required=True),
	"salesman_id":fields.Integer(required=True),
})
#---------------------------Add-Product-Into-Inventory------------------------------------#
@name_space.route("/AddProductIntoInventory")
class AddProductIntoInventory(Resource):
	@api.expect(product_inventory_post_model)
	def post(self):
		connection = bakesandcakes()
		cursor = connection.cursor()
		details = request.get_json()

		product_id = details['product_id']
		qty = details['qty']		
		inventory_id = details['inventory_id']
		last_update_id = details['last_update_id']
		

		transaction_type = 2

		insert_product_transaction_query = ("""INSERT INTO `product_trsansaction`(`transaction_type` , `inventory_id` , `last_update_id`) VALUES(%s,%s,%s)""")
		product_transaction_data = (transaction_type,inventory_id,last_update_id)
		cursor.execute(insert_product_transaction_query,product_transaction_data)

		transaction_id = cursor.lastrowid

		insert_query = ("""INSERT INTO `product_transaction_details`(`transaction_id` ,`product_id` ,`qty` ,`last_update_id`) VALUES(%s,%s,%s,%s)""")
		data = (transaction_id,product_id,qty,last_update_id)
		cursor.execute(insert_query,data)

		get_product_inventory_query = ("""SELECT * FROM `product_inventory_mapping` where `product_id` = %s and `inventory_id` = %s""")	
		get_product_inventory_data = (product_id,inventory_id)
		product_inventory_cont = cursor.execute(get_product_inventory_query,get_product_inventory_data)	

		if product_inventory_cont > 0:
			update_query = ("""UPDATE `product_inventory_mapping` SET `qty` = %s WHERE `product_id` = %s and `inventory_id` = %s""")
			update_data = (qty,product_id,inventory_id)
			cursor.execute(update_query,update_data)

		else:
			insert_product_inventory_query =("""INSERT INTO `product_inventory_mapping`(`product_id` , `qty` , `inventory_id`, `last_update_id`) VALUES(%s,%s,%s,%s)""")
			insert_product_data = (product_id,qty,inventory_id,last_update_id)
			cursor.execute(insert_product_inventory_query,insert_product_data)

		connection.commit()
		cursor.close()
		
		return ({"attributes": {
	    				"status_desc": "Product Inventory",
	    				"status": "success"	    				
	    				},
	    				"responseList":details}), status.HTTP_200_OK

#---------------------------Add-Product-Into-Inventory------------------------------------#

#---------------------------Sell-Product------------------------------------#
@name_space.route("/sellProductToRetailer")
class sellProductToRetailer(Resource):
	@api.expect(sell_product_post_model)
	def post(self):
		connection = bakesandcakes()
		cursor = connection.cursor()
		details = request.get_json()

		retailer_id = details['retailer_id']
		sales_amount = details['sales_amount']
		salesman_id = details['salesman_id']
		payment_mode = 'online'

		is_payment = 1
		order_status = 1
		
		products = details['products']

		now = datetime.now()
		today_date = now.strftime("%Y-%m-%d")
		today_date_time = now.strftime("%Y-%m-%d  %H:%M:%S")

		insert_sales_order_query = ("""INSERT INTO `sales_order`(`retailer_id` , `sales_man_id` , `sales_amount` ,`is_payment`, `order_date`,`order_status`,`last_update_id`) VALUES(%s,%s,%s,%s,%s,%s,%s)""")
		sales_order_data = (retailer_id,salesman_id,sales_amount,is_payment,today_date,order_status,salesman_id)
		cursor.execute(insert_sales_order_query,sales_order_data)
		sales_order_id = cursor.lastrowid
		details['sales_order_id'] = sales_order_id

		payment_type = 1

		insert_payment_transaction_query = ("""INSERT INTO `payment_transaction`(`sales_order_id` , `payment_type`,`payment_amount` ,`payment_mode`, `retailer_id` , `salesman_id`,`last_update_id`,`last_update_ts`) VALUES(%s,%s,%s,%s,%s,%s,%s,%s)""")
		payment_transaction_data = (sales_order_id,payment_type,sales_amount,payment_mode,retailer_id,salesman_id,salesman_id,today_date_time)
		cursor.execute(insert_payment_transaction_query,payment_transaction_data)

		

		for key,data in enumerate(products):
			insert_sales_order_product = ("""INSERT INTO `sales_order_product`(`sales_order_id` , `retailer_id`,`salesman_id`,`product_id` , `qty` ,`order_status`, `last_update_id`) VALUES(%s,%s,%s,%s,%s,%s,%s)""")
			sales_order_product_data = (sales_order_id,retailer_id,salesman_id,data['product_id'],data['qty'],order_status,salesman_id)
			cursor.execute(insert_sales_order_product,sales_order_product_data)	
		

		connection.commit()
		cursor.close()

		return ({"attributes": {
	    				"status_desc": "Sell Product To Retailer",
	    				"status": "success"	    				
	    				},
	    				"responseList":details}), status.HTTP_200_OK

#---------------------------Sell-Product------------------------------------#


#---------------------------Full-Filled-By-Retailer------------------------------------#
@name_space.route("/fullfilledbyRetailer")
class fullfilledbyRetailer(Resource):
	@api.expect(full_filled_post_model)
	def post(self):
		connection = bakesandcakes()
		cursor = connection.cursor()
		details = request.get_json()

		sales_order_id = details['sales_order_id']
		inventory_id = details['inventory_id']
		salesman_id = details['salesman_id']
		products = details['products']

		update_query = ("""UPDATE `sales_order` SET `is_fullfilled` = 1 WHERE `sales_order_id` = %s""")
		update_data = (sales_order_id)
		cursor.execute(update_query,update_data)

		'''payment_transaction_insert_query = ("""INSERT INTO `payment_transaction`(`sales_order_id` , `payment_type`, `last_update_id`) VALUES(%s,%s,%s)""")
		payment_transaction_data = (sales_order_id,payment_type,salesman_id)
		cursor.execute(payment_transaction_insert_query,payment_transaction_data)'''

		get_sales_order_query = ("""SELECT * FROM `sales_order` where `sales_order_id` = %s""")
		get_sales_order_data = (sales_order_id)
		sales_order_count = cursor.execute(get_sales_order_query,get_sales_order_data)
		sales_order_data = cursor.fetchone()	

		transaction_type = 1

		insert_product_transaction_query = ("""INSERT INTO `product_trsansaction`(`retailer_id`,`transaction_type` , `sales_order_id`,`salesman_id`,`inventory_id` , `last_update_id`) VALUES(%s,%s,%s,%s,%s,%s)""")
		product_transaction_data = (sales_order_data['retailer_id'],transaction_type,sales_order_id,sales_order_data['sales_man_id'],inventory_id,salesman_id)
		product_transaction_count = cursor.execute(insert_product_transaction_query,product_transaction_data)
		transaction_id = cursor.lastrowid

		get_sales_order_product_query = ("""SELECT * FROM `sales_order_product` where `sales_order_id` = %s""")
		get_sales_order_product_data = (sales_order_id)
		sales_order_product_count = cursor.execute(get_sales_order_product_query,get_sales_order_product_data)

		if sales_order_product_count > 0:
			sales_order_product = cursor.fetchall()		
			
			for key,data in enumerate(sales_order_product):				

				insert_transaction_details_query = ("""INSERT INTO `product_transaction_details`(`transaction_id` ,`product_id` ,`qty` ,`last_update_id`) VALUES(%s,%s,%s,%s)""")
				insert_transaction_details_data = (transaction_id,data['product_id'],data['qty'],salesman_id)
				cursor.execute(insert_transaction_details_query,insert_transaction_details_data)

				get_product_inventory_query = ("""SELECT * FROM `product_inventory_mapping` where `product_id` = %s and `inventory_id` = %s""")	
				get_product_inventory_data = (data['product_id'],inventory_id)
				product_inventory_cont = cursor.execute(get_product_inventory_query,get_product_inventory_data)

				if product_inventory_cont > 0:
					product_inventory_data = cursor.fetchone()
					qty = product_inventory_data['qty'] - data['qty']

					update_query = ("""UPDATE `product_inventory_mapping` SET `qty` = %s WHERE `product_id` = %s and `inventory_id` = %s""")
					update_data = (qty,data['product_id'],inventory_id)
					cursor.execute(update_query,update_data)

		connection.commit()
		cursor.close()

		return ({"attributes": {
	    				"status_desc": "Sell Product To Retailer",
	    				"status": "success"	    				
	    				},
	    				"responseList":details}), status.HTTP_200_OK

#---------------------------Full-Filled-By-Retailer------------------------------------#

#---------------------------Full-Filled-By-Retailer------------------------------------#
@name_space.route("/fullfilledAndPartialFullFillbyRetailer")
class fullfilledAndPartialFullFillbyRetailer(Resource):
	@api.expect(full_filled_and_partial_fullfill_post_model)
	def post(self):
		connection = bakesandcakes()
		cursor = connection.cursor()
		details = request.get_json()

		sales_order_id = details['sales_order_id']
		inventory_id = details['inventory_id']
		salesman_id = details['salesman_id']
		is_fullfill = details['is_fullfill']
		products = details['products']

		if is_fullfill == 1:

			update_query = ("""UPDATE `sales_order` SET `is_fullfilled` = 1 WHERE `sales_order_id` = %s""")
			update_data = (sales_order_id)
			cursor.execute(update_query,update_data)

			'''payment_transaction_insert_query = ("""INSERT INTO `payment_transaction`(`sales_order_id` , `payment_type`, `last_update_id`) VALUES(%s,%s,%s)""")
			payment_transaction_data = (sales_order_id,payment_type,salesman_id)
			cursor.execute(payment_transaction_insert_query,payment_transaction_data)'''

			get_sales_order_query = ("""SELECT * FROM `sales_order` where `sales_order_id` = %s""")
			get_sales_order_data = (sales_order_id)
			sales_order_count = cursor.execute(get_sales_order_query,get_sales_order_data)
			sales_order_data = cursor.fetchone()	

			transaction_type = 1

			insert_product_transaction_query = ("""INSERT INTO `product_trsansaction`(`retailer_id`,`transaction_type` , `sales_order_id`,`salesman_id`,`inventory_id` , `last_update_id`) VALUES(%s,%s,%s,%s,%s,%s)""")
			product_transaction_data = (sales_order_data['retailer_id'],transaction_type,sales_order_id,sales_order_data['sales_man_id'],inventory_id,salesman_id)
			product_transaction_count = cursor.execute(insert_product_transaction_query,product_transaction_data)
			transaction_id = cursor.lastrowid

			get_sales_order_product_query = ("""SELECT * FROM `sales_order_product` where `sales_order_id` = %s""")
			get_sales_order_product_data = (sales_order_id)
			sales_order_product_count = cursor.execute(get_sales_order_product_query,get_sales_order_product_data)

			if sales_order_product_count > 0:
				sales_order_product = cursor.fetchall()		
				
				for key,data in enumerate(sales_order_product):				

					insert_transaction_details_query = ("""INSERT INTO `product_transaction_details`(`transaction_id` ,`product_id` ,`qty` ,`last_update_id`) VALUES(%s,%s,%s,%s)""")
					insert_transaction_details_data = (transaction_id,data['product_id'],data['qty'],salesman_id)
					cursor.execute(insert_transaction_details_query,insert_transaction_details_data)

					get_product_inventory_query = ("""SELECT * FROM `product_inventory_mapping` where `product_id` = %s and `inventory_id` = %s""")	
					get_product_inventory_data = (data['product_id'],inventory_id)
					product_inventory_cont = cursor.execute(get_product_inventory_query,get_product_inventory_data)

					if product_inventory_cont > 0:
						product_inventory_data = cursor.fetchone()
						qty = product_inventory_data['qty'] - data['qty']

						update_query = ("""UPDATE `product_inventory_mapping` SET `qty` = %s WHERE `product_id` = %s and `inventory_id` = %s""")
						update_data = (qty,data['product_id'],inventory_id)
						cursor.execute(update_query,update_data)
		else:
			update_query = ("""UPDATE `sales_order` SET `is_fullfilled` = 2 WHERE `sales_order_id` = %s""")
			update_data = (sales_order_id)
			cursor.execute(update_query,update_data)			

			get_sales_order_query = ("""SELECT * FROM `sales_order` where `sales_order_id` = %s""")
			get_sales_order_data = (sales_order_id)
			sales_order_count = cursor.execute(get_sales_order_query,get_sales_order_data)
			sales_order_data = cursor.fetchone()

			transaction_type = 1

			insert_product_transaction_query = ("""INSERT INTO `product_trsansaction`(`retailer_id`,`transaction_type` , `sales_order_id`,`salesman_id`,`inventory_id` , `last_update_id`) VALUES(%s,%s,%s,%s,%s,%s)""")
			product_transaction_data = (sales_order_data['retailer_id'],transaction_type,sales_order_id,sales_order_data['sales_man_id'],inventory_id,salesman_id)
			product_transaction_count = cursor.execute(insert_product_transaction_query,product_transaction_data)
			transaction_id = cursor.lastrowid

			for key,data in enumerate(products):
				insert_partial_fullfill_product_query = ("""INSERT INTO `partial_fullfill_product`(`sales_order_id` ,`retailer_id` ,`salesman_id` ,`product_id`,`qty`,`last_update_id`) VALUES(%s,%s,%s,%s,%s,%s)""")
				insert_partial_fullfill_product_data = (sales_order_id,sales_order_data['retailer_id'],salesman_id,data['product_id'],data['qty'],salesman_id)
				cursor.execute(insert_partial_fullfill_product_query,insert_partial_fullfill_product_data)

				insert_transaction_details_query = ("""INSERT INTO `product_transaction_details`(`transaction_id` ,`product_id` ,`qty` ,`last_update_id`) VALUES(%s,%s,%s,%s)""")
				insert_transaction_details_data = (transaction_id,data['product_id'],data['qty'],salesman_id)
				cursor.execute(insert_transaction_details_query,insert_transaction_details_data)

				get_product_inventory_query = ("""SELECT * FROM `product_inventory_mapping` where `product_id` = %s and `inventory_id` = %s""")	
				get_product_inventory_data = (data['product_id'],inventory_id)
				product_inventory_cont = cursor.execute(get_product_inventory_query,get_product_inventory_data)

				if product_inventory_cont > 0:
					product_inventory_data = cursor.fetchone()
					qty = product_inventory_data['qty'] - data['qty']

					update_query = ("""UPDATE `product_inventory_mapping` SET `qty` = %s WHERE `product_id` = %s and `inventory_id` = %s""")
					update_data = (qty,data['product_id'],inventory_id)
					cursor.execute(update_query,update_data)

		connection.commit()
		cursor.close()

		return ({"attributes": {
	    				"status_desc": "Full Fill By Retailer",
	    				"status": "success"	    				
	    				},
	    				"responseList":details}), status.HTTP_200_OK

#---------------------------Full-Filled-By-Retailer------------------------------------#

#---------------------------Paid-By-Admin------------------------------------#
@name_space.route("/paidByAdmin")
class paidByAdmin(Resource):
	@api.expect(is_paid_post_model)
	def post(self):
		connection = bakesandcakes()
		cursor = connection.cursor()
		details = request.get_json()

		sales_order_id = details['sales_order_id']
		payment_type = details['payment_type']
		transaction_id = details['transaction_id']
		salesman_id = details['salesman_id']

		update_query = ("""UPDATE `sales_order` SET `is_payment` = 1 WHERE `sales_order_id` = %s""")
		update_data = (sales_order_id)
		cursor.execute(update_query,update_data)

		payment_transaction_insert_query = ("""INSERT INTO `payment_transaction`(`sales_order_id` , `payment_type`, `last_update_id`) VALUES(%s,%s,%s)""")
		payment_transaction_data = (sales_order_id,payment_type,salesman_id)
		cursor.execute(payment_transaction_insert_query,payment_transaction_data)

		connection.commit()
		cursor.close()

		return ({"attributes": {
	    				"status_desc": "Sales Order Piad By Admin",
	    				"status": "success"	    				
	    				},
	    				"responseList":details}), status.HTTP_200_OK

#---------------------------Paid-By-Admin------------------------------------#



#---------------------------Product-Type-List------------------------------------#

@name_space.route("/productTypeList")	
class productTypeList(Resource):
	def get(self):
		connection = bakesandcakes()
		cursor = connection.cursor()

		get_product_type_query = ("""SELECT *
				FROM `product_type_master`""")
		product_type_count = cursor.execute(get_product_type_query)

		if product_type_count > 0:
			product_type_list = cursor.fetchall()
			for key,data in enumerate(product_type_list):
				product_type_list[key]['last_update_ts'] = str(data['last_update_ts'])
		else:
			product_type_list = []

		return ({"attributes": {
	    				"status_desc": "Product Type List",
	    				"status": "success"	    				
	    				},
	    				"responseList":product_type_list}), status.HTTP_200_OK



#---------------------------Product-Type-List------------------------------------#

#---------------------------Add-Product------------------------------------#
@name_space.route("/AddProduct")
class AddProduct(Resource):
	@api.expect(product_post_model)
	def post(self):
		connection = bakesandcakes()
		cursor = connection.cursor()

		details = request.get_json()

		product_name = details['product_name']
		product_type_id =details['product_type_id']
		in_price = details['in_price']
		out_price = details['out_price']
		last_update_id = details['last_update_id']

		get_product_query = ("""SELECT * FROM `product` where `product_name` = %s""")
		get_product_data = (product_name)
		count_product = cursor.execute(get_product_query,get_product_data)

		if count_product > 0:
			return ({"attributes": {
				    		"status_desc": "product_details",
				    		"status": "error"
				    	},
				    	"responseList":"Product Already Exsits" }), status.HTTP_200_OK
		else:
			insert_query = ("""INSERT INTO `product`(`product_name` , `product_type_id` , `in_price` , `out_price` , `last_update_id`) VALUES(%s,%s,%s,%s,%s)""")
			data = (product_name,product_type_id,in_price,out_price,last_update_id)
			cursor.execute(insert_query,data)

			details['product_id'] = cursor.lastrowid

			return ({"attributes": {
	    				"status_desc": "Product Details.",
	    				"status": "success",
	    				
	    				},
	    				"responseList":details}), status.HTTP_200_OK

#---------------------------Add-Product------------------------------------#

#---------------------------Add-Salesman------------------------------------#
@name_space.route("/AddSalesman")
class AddSalesman(Resource):
	@api.expect(salesman_post_model)
	def post(self):
		connection = bakesandcakes()
		cursor = connection.cursor()

		details = request.get_json()

		name = details['name']
		phoneno = details['phoneno']
		password = details['password']
		secondary_number = details['secondary_number']
		admin_id = details['admin_id']
		role = 2

		get_salesman_query = ("""SELECT * FROM `user` where `phoneno` = %s and `admin_id` = %s""")
		get_salesman_data = (phoneno,admin_id)
		salesman_count = cursor.execute(get_salesman_query,get_salesman_data)

		if salesman_count > 0:
			return ({"attributes": {
				    		"status_desc": "salesman_details",
				    		"status": "error"
				    	},
				    	"responseList":"Sales Man Already Exsits" }), status.HTTP_200_OK
		else:
			insert_query = ("""INSERT INTO `user`(`name` , `phoneno` , `password` , `secondary_number` , `role`,`admin_id`,`last_update_id`) VALUES(%s,%s,%s,%s,%s,%s,%s)""")
			data = (name,phoneno,password,secondary_number,role,admin_id,admin_id)
			cursor.execute(insert_query,data)

			details['user_id'] = cursor.lastrowid

			return ({"attributes": {
	    				"status_desc": "Product Details.",
	    				"status": "success",
	    				
	    				},
	    				"responseList":details}), status.HTTP_200_OK

#---------------------------Add-Salesman------------------------------------#

#---------------------------Salesman-Dashboard------------------------------------#

@name_space_salesman_dashboard.route("/salesmandashboard")	
class salesmandashboard(Resource):
	def get(self):
		connection = bakesandcakes()
		cursor = connection.cursor()


		salesman_data = {}

		get_total_no_of_absent_salesman_query = ("""SELECT count(distinct(salesman_id)) as total_no_of_absent_salesman FROM `salesman_attendance` where `attendance_type_id` = 2""")
		absent_salesman_count = cursor.execute(get_total_no_of_absent_salesman_query)

		if absent_salesman_count > 0:
			absent_salesman_data = cursor.fetchone()
			salesman_data['get_total_no_of_absent'] = absent_salesman_data['total_no_of_absent_salesman']
		else:
			salesman_data['get_total_no_of_absent'] = 0

		return ({"attributes": {
	    				"status_desc": "Product Details.",
	    				"status": "success",
	    				
	    				},
	    				"responseList":details}), status.HTTP_200_OK

#---------------------------Salesman-Dashboard------------------------------------#

#---------------------------Sell-Product------------------------------------#
@name_space.route("/returnProductFromRetailer")
class sellProductToRetailer(Resource):
	@api.expect(sell_product_post_model)
	def post(self):
		connection = bakesandcakes()
		cursor = connection.cursor()
		details = request.get_json()

		retailer_id = details['retailer_id']
		sales_amount = details['sales_amount']
		salesman_id = details['salesman_id']
		payment_mode = "online"
		order_status = 2
		is_payment = 1
		
		products = details['products']

		now = datetime.now()
		today_date = now.strftime("%Y-%m-%d")
		today_date_time = now.strftime("%Y-%m-%d  %H:%M:%S")

		insert_sales_order_query = ("""INSERT INTO `sales_order`(`retailer_id` , `sales_man_id` , `sales_amount` ,`is_payment`, `order_date`,`order_status`,`last_update_id`) VALUES(%s,%s,%s,%s,%s,%s,%s)""")
		sales_order_data = (retailer_id,salesman_id,sales_amount,is_payment,today_date,order_status,salesman_id)
		cursor.execute(insert_sales_order_query,sales_order_data)
		sales_order_id = cursor.lastrowid
		details['sales_order_id'] = sales_order_id

		payment_type = 2

		insert_payment_transaction_query = ("""INSERT INTO `payment_transaction`(`sales_order_id` , `payment_type`,`payment_amount` , `payment_mode`,`retailer_id` , `salesman_id`,`last_update_id`,`last_update_ts`) VALUES(%s,%s,%s,%s,%s,%s,%s,%s)""")
		payment_transaction_data = (sales_order_id,payment_type,sales_amount,payment_mode,retailer_id,salesman_id,salesman_id,today_date_time)
		cursor.execute(insert_payment_transaction_query,payment_transaction_data)

		

		for key,data in enumerate(products):
			insert_sales_order_product = ("""INSERT INTO `sales_order_product`(`sales_order_id` , `retailer_id`,`salesman_id`,`product_id` , `qty` , `order_status`,`last_update_id`) VALUES(%s,%s,%s,%s,%s,%s,%s)""")
			sales_order_product_data = (sales_order_id,retailer_id,salesman_id,data['product_id'],data['qty'],order_status,salesman_id)
			cursor.execute(insert_sales_order_product,sales_order_product_data)	
		

		connection.commit()
		cursor.close()

		return ({"attributes": {
	    				"status_desc": "Return Product From Retailer",
	    				"status": "success"	    				
	    				},
	    				"responseList":details}), status.HTTP_200_OK

#---------------------------Sell-Product------------------------------------#

#---------------------------Create-Payment------------------------------------#
@name_space.route("/createPayment")
class createPayment(Resource):
	@api.expect(payment_post_model)
	def post(self):
		connection = bakesandcakes()
		cursor = connection.cursor()
		details = request.get_json()
		
		payment_type = details['payment_type']
		payment_amount = details['payment_amount']
		payment_mode = details['payment_mode']
		retailer_id = details['retailer_id']
		salesman_id = details['salesman_id']	
		now = datetime.now()
		today_date_time = now.strftime("%Y-%m-%d  %H:%M:%S")

		payment_transaction_insert_query = ("""INSERT INTO `payment_transaction`(`payment_type`, `payment_amount`,`payment_mode`,`retailer_id`,`salesman_id`,`last_update_id`,`last_update_ts`) VALUES(%s,%s,%s,%s,%s,%s,%s)""")
		payment_transaction_data = (payment_type,payment_amount,payment_mode,retailer_id,salesman_id,salesman_id,today_date_time)
		cursor.execute(payment_transaction_insert_query,payment_transaction_data)

		connection.commit()
		cursor.close()

		return ({"attributes": {
	    				"status_desc": "Create Payment",
	    				"status": "success"	    				
	    				},
	    				"responseList":details}), status.HTTP_200_OK

#---------------------------Create-Payment------------------------------------#

#---------------------------get-Payment-History------------------------------------#

@name_space.route("/getPyamnetHistory/<int:retailer_id>/<int:salesman_id>")	
class getPyamnetHistory(Resource):
	def get(self,retailer_id,salesman_id):
		connection = bakesandcakes()
		cursor = connection.cursor()

		get_payment_history_query = ("""SELECT * FROM `payment_transaction` where `retailer_id` = %s and `salesman_id` = %s""")
		get_payment_history_data = (retailer_id,salesman_id)
		payment_history_count = cursor.execute(get_payment_history_query,get_payment_history_data)
		if payment_history_count > 0:
			payment_history_data = cursor.fetchall()

			for paymentkey,paymentdata in enumerate(payment_history_data):
				get_sales_order_query = ("""SELECT * FROM `sales_order` where `sales_order_id` = %s """)
				get_sales_order_data = (paymentdata['sales_order_id'])
				sales_order_count = cursor.execute(get_sales_order_query,get_sales_order_data)

				if sales_order_count > 0:
					sales_order_data = cursor.fetchone()
					
					sales_order_data['order_date'] = str(sales_order_data['order_date'])
					sales_order_data['last_update_ts'] = str(sales_order_data['last_update_ts'])

					get_sales_order_product_query = ("""SELECT sop.`product_id`,p.`product_name`,p.`in_price`,p.`out_price`,sop.`qty` 
							FROM `sales_order_product` sop 
							INNER JOIN `product` p ON p.`product_id` = sop.`product_id`
							where `sales_order_id` = %s""")
					get_sales_order_product_data = (paymentdata['sales_order_id'])
					sales_order_product_count = cursor.execute(get_sales_order_product_query,get_sales_order_product_data)
					if sales_order_product_count > 0:
						sales_order_product = cursor.fetchall()
						sales_order_data['order_product'] = sales_order_product
					else:
						sales_order_data['order_product'] = []

				else:
					sales_order_data = None

				payment_history_data[paymentkey]['sales_order'] = sales_order_data
				payment_history_data[paymentkey]['last_update_ts'] = str(paymentdata['last_update_ts'])

		return ({"attributes": {
				    		"status_desc": "Payment Order History",
				    		"status": "success"
				},
				"responseList":payment_history_data}), status.HTTP_200_OK


#---------------------------get-Payment-History------------------------------------#





