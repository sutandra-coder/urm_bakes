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
import haversine as hs
import requests
import calendar
import json
from threading import Thread
import time
import os   

app = Flask(__name__)
cors = CORS(app)

bakes_and_cakes = Blueprint('bakes_and_cakes_api', __name__)
api = Api(bakes_and_cakes,  title='BakesAndCakes API',description='Bakes And Cakes API')
name_space = api.namespace('BakesAndCakes',description='BakesAndCakes')

sales_post_model = api.model('PostSalesman',{
	"salesman_transaction_id":fields.Integer(),
	"salesman_id":fields.Integer(),
	"qty":fields.Integer(),
	"product_id":fields.Integer(),
	"last_update_id":fields.Integer()
})

document_model = api.model('document_model ', {
	"documents_type_id":fields.Integer(),
	"document_link": fields.String(),
	"document_no": fields.String(),
})


class DictModel(fields.Raw):
	def format(self, value):
		dictmodel = {}
		return dictmodel

retailer_detail_model = api.model('retailer_detail_model',{
	"retailer_name":fields.String(required=True),
	"logo":fields.String,
	"latitude":fields.String(required=True),
	"longitude":fields.String(required=True),
	"pincode":fields.String(),
	"city":fields.String(),
	"statte":fields.String(),
	"address":fields.String(),
	"owner_name":fields.String(),
	"whatsapp_no":fields.String(),
	"email":fields.String(),
	"salesman_id":fields.Integer(required=True),
	"documet_type":fields.List(fields.Nested(document_model )),
})


salesman_login_model = api.model('salesman_login_model',{
	"phoneno": fields.String(required=True),
	"password":fields.String(required=True)
})

salesman_attendance_model = api.model('salesman_attendance_model',{
	"attendance_type_id" : fields.Integer(),
	"salesman_id" : fields.Integer(),
	"attendance_date" : fields.String(),
	"attendance_time" : fields.String(),
	"last_update_id" : fields.Integer()
})

salesman_touchbase_model = api.model('salesman_touchbase_model',{
	"salesman_id" : fields.Integer(),
	"latitude" : fields.String(),
	"longitude" : fields.String(),
	"last_update_id" : fields.Integer(),
	"salesman_selfy_image" : fields.String(),
	"start_time":fields.String(),
	"retailer_id" : fields.Integer()
})

task_model = api.model('task_model ', {
	"task_id":fields.Integer(),
	"status": fields.Integer()
})

retailer_checkout_model = api.model('retailer_checkout_model',{
	"salesman_retailer_task_type": fields.List(fields.Nested(task_model )),
	"note":fields.String,
	"touchbase_end_time":fields.String(),
	"total_time_spent":fields.String()
})


task_put_model = api.model('task_put_model',{
	"is_complte": fields.Integer()
})
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

#----------------------Inventory-List---------------------#

@name_space.route("/InventoryList")	
class InventoryList(Resource):
	def get(self):
		connection = bakesandcakes()
		cursor = connection.cursor()

		get_query = ("""SELECT *
					FROM `inventory_master`""")
		cursor.execute(get_query)
		Inventory_list = cursor.fetchall()

		for key,data in enumerate(Inventory_list):
			Inventory_list[key]['last_update_ts'] = str(data['last_update_ts'])


		return ({"attributes": {"status_desc": "Inventory List",
	                            "status": "success"
	                            },
	             "responseList": Inventory_list}), status.HTTP_200_OK

#----------------------Inventory-List---------------------#

#----------------------Product-List---------------------#

@name_space.route("/ProductList/<int:inventory_id>")	
class ProductList(Resource):
	def get(self,inventory_id):
		connection = bakesandcakes()
		cursor = connection.cursor()

		get_query = ("""SELECT *
					FROM `product`""")
		cursor.execute(get_query)
		product_list = cursor.fetchall()

		for key,data in enumerate(product_list):
			get_product_qty_by_inventory = ("""SELECT *
					FROM `product_inventory_mapping` where `inventory_id` = %s and `product_id` = %s""")
			get_product_qty_data = (inventory_id,data['product_id'])
			count_product_qty = cursor.execute(get_product_qty_by_inventory,get_product_qty_data)
			if count_product_qty > 0:
				product_inventoty_qty = cursor.fetchone()
				product_list[key]['qty'] = product_inventoty_qty['qty']
			else:
				product_list[key]['qty'] = 0

			product_list[key]['last_update_ts'] = str(data['last_update_ts'])


		return ({"attributes": {"status_desc": "Inventory List",
	                            "status": "success"
	                            },
	             "responseList": product_list}), status.HTTP_200_OK

#----------------------Product-List---------------------#

@name_space.route("/ProductListBySalesmanId/<int:salesman_id>")	
class ProductListBySalesmanId(Resource):
	def get(self,salesman_id):
		connection = bakesandcakes()
		cursor = connection.cursor()
		
		
		cursor.execute("""SELECT salesman_transaction.`salesman_transaction_id`,salesman_transaction.`salesman_id`,product.`product_id`,product.`product_name`,product.`qty`,product.`in_price`,product.`out_price`,product.`last_update_id` FROM salesman_transaction INNER JOIN product ON product.`product_id` = salesman_transaction.`product_id` WHERE salesman_transaction.`salesman_id` = %s """,(salesman_id))

		ProductList = cursor.fetchall()
		if ProductList == ():
			ProductList = []
			
		else:
			ProductList = ProductList
				
							
		connection.commit()
		cursor.close()

		return ({"attributes": {"status_desc": "Product List",
	                            "status": "success"
	                            },
	             "responseList": ProductList}), status.HTTP_200_OK

#-------------------------------------------------------get api for product---------------------------------------------------------------------#



@name_space.route("/productDetails/<int:product_id>")	
class productDetails(Resource):
	def get(self,product_id):
		connection = bakesandcakes()
		cursor = connection.cursor()
		details=[]
		in_price=[]
		outprice=[]

		cursor.execute("""SELECT `product_id`,`product_name`,`qty` FROM `product` WHERE `product_id` = %s""",(product_id))
		details=cursor.fetchone()

		quantity = details["qty"]

		cursor.execute("""SELECT `in_price` FROM `product` WHERE `product_id` = %s""",(product_id))
		in_price=cursor.fetchone()

		inprice = in_price["in_price"]

		cursor.execute("""SELECT `out_price` FROM `product` WHERE `product_id` = %s""",(product_id))
		out_price=cursor.fetchone()

		outprice = out_price["out_price"]

		in_price_per_unit = inprice/quantity
		out_price_per_unit = outprice/quantity

		details["in_price_per_unit"]=in_price_per_unit
		details["out_price_per_unit"] = out_price_per_unit

		return ({"attributes": {"status_desc": "Product List",
	                            "status": "success"
	                            },
	             "responseList": details}), status.HTTP_200_OK

#--------------------------------------------------------------------retailer post api----------------------------------------------------------------------#

@name_space.route("/addRetailer")
class addRetailer(Resource):
	@api.expect(retailer_detail_model)
	def post(self):
		connection = bakesandcakes()
		cursor = connection.cursor()
		details = request.get_json()		

		name=details["retailer_name"]
		logo = details['logo']
		latitude = details["latitude"]
		longitude = details["longitude"]
		pincode=details["pincode"]
		city=details["city"]
		statte=details["statte"]
		location=details["address"]
		owner_name=details["owner_name"]
		whatsapp_no=details["whatsapp_no"]
		email=details["email"]
		salesman_id = details['salesman_id']

		documet_type = details['documet_type']

		print(documet_type)

		now = datetime.now()
		today_date = now.strftime("%Y-%m-%d %H:%M:%S")

		
		retailer_query = ("""INSERT INTO `retailer`(`name`,`latitude`,`longitude`,`owner_name`,`logo`,`whatsapp_no`,`email`,`location`,`city`,`state`,`pincode`,`last_update_id`,`last_update_ts`) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""")
		retailer_data = (name,latitude,longitude,owner_name,logo,whatsapp_no,email,location,city,statte,pincode,salesman_id,today_date)
		retailer_insert = cursor.execute(retailer_query,retailer_data)	

		retailer_id = cursor.lastrowid

		if 	retailer_id > 0:
			retailer_salesman_mapping_query = ("""INSERT INTO `retailer_salesman_mapping`(`retailer_id`,`salesman_id`,`last_update_id`,`last_update_ts`) VALUES (%s,%s,%s,%s)""")
			retailer_salesman_mapping_data = (retailer_id,salesman_id,salesman_id,today_date)
			retailer_salesman_mapping = cursor.execute(retailer_salesman_mapping_query,retailer_salesman_mapping_data)

			for key,data in enumerate(documet_type):
				retailer_documents_query = ("""INSERT INTO `retailer_documents`(`documents_type_id`,`document_link`,`document_no`,`retailer_id`,`last_update_id`,`last_update_ts`) VALUES (%s,%s,%s,%s,%s,%s)""")
				retailer_documents_data = (data['documents_type_id'],data['document_link'],data['document_no'],retailer_id,salesman_id,today_date)
				retailer_documents_count = cursor.execute(retailer_documents_query,retailer_documents_data)


			connection.commit()
			cursor.close()	

			return ({"attributes": {"status_desc": "Retailer Details",
                                "status": "success",
                                "msg": ""
                                
                                },
	             "responseList": details}), status.HTTP_200_OK
		else:
			return ({"attributes": {"status_desc": "Retailer Details",
                                "status": "error",
                                "msg": "Retailer Not Inserted"
                                
                                },
	             "responseList": {}}), status.HTTP_200_OK

		

		


#-----------------------------------------------------------------------get product avalibility------------------------------------------------------------------------------#
@name_space.route("/availabeProductlistInSalesmanBag/<int:salesman_id>/<int:product_id>")	
class availabeProductlistInSalesmanBag(Resource):
	def get(self,salesman_id,product_id):
		connection = bakesandcakes()
		cursor = connection.cursor()

		cursor.execute("""SELECT `qty` FROM `salesman_transaction` WHERE `salesman_id` =%s AND `product_id`=%s """,(salesman_id,product_id))
		quantity_list=cursor.fetchone()

		cursor.execute("""SELECT `product_id`,`qty`,`product_name`,`in_price`,`out_price` FROM `product` WHERE `product_id` = %s""",(product_id))
		product_list=cursor.fetchone()

		product_list["available_quantity"]=quantity_list["qty"]

		cursor.close()



		return ({"attributes": {"status_desc": "Product List",
	                            "status": "success"
	                            },
	             "responseList": product_list}), status.HTTP_200_OK

#----------------------------------product-retailer get api------------------------------------------------#

#----------------------------------------------get api for product and retailer details by salesman_id--------------------------------------------#

@name_space.route("/Selldetailsbysalesmanid/<int:salesman_id>")	
class Selldetailsbysalesmanid(Resource):
	def get(self,salesman_id):
		connection = bakesandcakes()
		cursor = connection.cursor()
		details=[]

		cursor.execute("""SELECT `retailer_id` FROM `retailer_salesman_mapping` WHERE `salesman_id`=%s""",(salesman_id))
		retailer = cursor.fetchone()

		retailerid = retailer["retailer_id"]

		cursor.execute("""SELECT `retailer_id`,`name`,`phoneno`,`whatsapp_no`,`location`,`latitude`,`longitude` FROM `retailer` WHERE `retailer_id`=%s""",(retailerid))
		retailerdetails=cursor.fetchone()

		cursor.execute("""SELECT `product_id` FROM `salesman_transaction` WHERE `salesman_id`=%s""",(salesman_id))
		productlist=cursor.fetchall()

		for i in productlist:
			details.append(i["product_id"])
		product=tuple(details)

		cursor.execute("""SELECT `product_id`,`product_name`,`qty` FROM `product` WHERE `product_id` IN (%s)""",(product))
		products=cursor.fetchall()

		for i in products:
			retailerdetails["product_id"]=i["product_id"]
			retailerdetails["product_name"]=i["product_name"]
			retailerdetails["qty"]=i["qty"]

		return ({"attributes": {"status_desc": "Retailer Details",
                                "status": "success",
                                
                                
                                },
	             "responseList": retailerdetails}), status.HTTP_200_OK




#----------------------------------Salesman-Login------------------------------------------------#
@name_space.route("/salesmanlogin")
class salesmanlogin(Resource):
	@api.expect(salesman_login_model)
	def post(self):
		connection = bakesandcakes()
		cursor = connection.cursor()
		details = request.get_json()
		phoneno = details["phoneno"]
		password = details["password"]
		
		salesman_count = cursor.execute("""SELECT `user_id`,`name`,`phoneno`,`password`,`secondary_number`,`role`,`admin_id`,`last_update_id` FROM `user` WHERE `role` = '2' AND `password`=%s AND `phoneno`=%s""",(phoneno,password))
		salesmandetail = cursor.fetchone()
		if salesman_count < 1:
			msg = "NOT EXISTED"
			return ({"attributes": {
				    		"status_desc": "salesman_details",
				    		"status": "error",
				    		"message":"Wrong Credentials"
				    	},
				    	"responseList":{} }), status.HTTP_200_OK
		else:
			return ({"attributes": {"status_desc": "Salesman Login Detail",
                                "status": "success",
                                },
	             "responseList": salesmandetail}), status.HTTP_200_OK


#----------------------------------Salesman-Login------------------------------------------------#

#----------------------------------Task-Type-List-By-Salesman-Id------------------------------------------------#

@name_space.route("/Tasktypelistbysalesmanid/<int:salesman_id>/<string:filter_date>")	
class Tasktypelistbysalesmanid(Resource):
	def get(self,salesman_id,filter_date):
		connection = bakesandcakes()
		cursor = connection.cursor()
		
		get_task_type_query = ("""SELECT *
					FROM `task_type`""")
		get_task_type_count = cursor.execute(get_task_type_query)

		if get_task_type_count > 0:

			task_type_data = cursor.fetchall()
			for key,data in enumerate(task_type_data):
				task_type_data[key]['task_update_ts'] = str(data['task_update_ts'])

				get_sales_man_task_completed_count_query = ("""SELECT count(*) as sales_man_completed_task_count
					FROM `salesman_task` where `salesman_id` = %s and `task_type_id` = %s and `is_complete` = 1 and DATE(`task_date`) = %s""")
				get_sales_man_task_completed_count_data = (salesman_id,data['task_type_id'],filter_date)
				get_sales_man_task_completed_count = cursor.execute(get_sales_man_task_completed_count_query,get_sales_man_task_completed_count_data)

				if get_sales_man_task_completed_count > 0:
					sales_man_task_completed_data = cursor.fetchone()
					task_type_data[key]['completed_count'] = sales_man_task_completed_data['sales_man_completed_task_count']
				else:
					task_type_data[key]['completed_count'] = 0

				task_list_query = ("""SELECT `salesman_task_id`,`task_name`,`is_complete`,`assign_date`,`due_date`,`task_time` FROM `salesman_task` WHERE `salesman_id`=%s  AND `task_type_id` =%s and DATE(`task_date`) = %s""")
				task_list_data = (salesman_id,data['task_type_id'],filter_date)
				tasklist=cursor.execute(task_list_query,task_list_data)
				tasks=cursor.fetchall()
				print(tasks)
				for tkey,task in enumerate(tasks):
					if task["is_complete"] == 1:
						tasks[tkey]["is_complete"] = 'Y'
					elif  task["is_complete"] == 0:
						tasks[tkey]["is_complete"] = 'N'
					
					tasks[tkey]['assign_date'] = str(task['assign_date'])
					tasks[tkey]['due_date'] = str(task['due_date'])					
				task_type_data[key]['tasks'] = tasks

		else:
			task_type_data = []

		return ({"attributes": {"status_desc": "Task Detail",
                                "status": "success",
                                },
	             "responseList": task_type_data}), status.HTTP_200_OK


#----------------------------------Task-Type-List-By-Salesman-Id------------------------------------------------#

#----------------------------------Attendance-Type-List------------------------------------------------#

@name_space.route("/GetAttendanceTypes")	
class GetAttendanceTypes(Resource):
	def get(self):
		connection = bakesandcakes()
		cursor = connection.cursor()

		attendance_type_query = ("""SELECT * FROM `attendance_type_master`""")
		attendance_types = cursor.execute(attendance_type_query)
		attendance_type_list = cursor.fetchall()
		for key,data in enumerate(attendance_type_list):
			attendance_type_list[key]['last_update_ts'] = str(data['last_update_ts'])
		return ({"attributes": {"status_desc": "Attendance Type Detail",
                                "status": "success",
                                },
	             "responseList": attendance_type_list}), status.HTTP_200_OK


#----------------------------------Attendance-Type-List------------------------------------------------#


#----------------------------------Save-Sales-man-Attendance------------------------------------------------#

@name_space.route("/addsalesmanattendance")
class addsalesmanattendance(Resource):
	@api.expect(salesman_attendance_model)
	def post(self):
		connection = bakesandcakes()
		cursor = connection.cursor()
		salesman = request.get_json()
		attendance_type_id=salesman["attendance_type_id"]
		salesman_id = salesman["salesman_id"]
		attendance_date = salesman["attendance_date"]
		attendance_time = salesman["attendance_time"]
		last_update_id = salesman["last_update_id"]
		now = datetime.now()
		today_date = now.strftime("%Y-%m-%d %H:%M:%S")

		get_attendance_query = ("""SELECT * from `salesman_attendance` where `salesman_id` = %s and `attendance_date` = %s""")
		get_attendance_data = (salesman_id,attendance_date)
		get_attendance_count = cursor.execute(get_attendance_query,get_attendance_data)

		if get_attendance_count > 0:
			update_query = ("""UPDATE `salesman_attendance` SET `attendance_type_id` = %s, `attendance_time` = %s
							WHERE `salesman_id` = %s and `attendance_date` = %s""")
			update_data = (attendance_type_id,attendance_time,salesman_id,attendance_date)
			cursor.execute(update_query,update_data)

		else:
			insert_query = ("""INSERT INTO `salesman_attendance`(`attendance_type_id`,`salesman_id`,`attendance_date`,`attendance_time`,`last_update_id`,`last_update_ts`) VALUES(%s,%s,%s,%s,%s,%s)""")
			data=(attendance_type_id,salesman_id,attendance_date,attendance_time,last_update_id,today_date)
			insertdata=cursor.execute(insert_query,data)
		
		connection.commit()
		cursor.close()
		return ({"attributes": {
	    				"status_desc": "Salesman Attendance",
	    				"status": "success"
	    				},
	    				"responseList":salesman}), status.HTTP_200_OK


#----------------------------------Save-Sales-man-Attendance------------------------------------------------#

#----------------------------------Get-Salesman-Attendence------------------------------------------------#

@name_space.route("/GetSalesmanAttendence/<int:salesman_id>")	
class GetSalesmanAttendence(Resource):
	def get(self,salesman_id):
		connection = bakesandcakes()
		cursor = connection.cursor()

		now = datetime.now()
		today_date = now.strftime("%Y-%m-%d")
		#today_date = "2021-12-20"

		salesman_attendance_query = ("""SELECT atm.`attendance_type_id`,sa.`attendance_date`,sa.`attendance_time`,atm.`attendance_type_name` FROM `salesman_attendance` sa
										INNER JOIN `attendance_type_master` atm ON atm.`attendance_type_id` = sa.`attendance_type_id`	
										where DATE(`attendance_date`) = %s and `salesman_id` = %s""")
		salesman_attendance_data = (today_date,salesman_id)
		salesman_attendance_count = cursor.execute(salesman_attendance_query,salesman_attendance_data)
		if salesman_attendance_count > 0 :
			salesman_attendance = cursor.fetchone()
			salesman_attendance['attendance_date'] = str(salesman_attendance['attendance_date'])
			salesman_attendance['attendance_time'] = str(salesman_attendance['attendance_time'])
		else:
			salesman_attendance = None
		
		
		return ({"attributes": {"status_desc": "Salesman Attendance",
                                "status": "success",
                                },
	             "responseList": salesman_attendance}), status.HTTP_200_OK

#----------------------------------Get-Salesman-Attendence------------------------------------------------#


#----------------------------------Add-Sales-man-Touchbase------------------------------------------------#

@name_space.route("/salesmantouchbasepost")
class salesmantouchbasepost(Resource):
	@api.expect(salesman_touchbase_model)
	def post(self):
		connection = bakesandcakes()
		cursor = connection.cursor()
		salesman = request.get_json()
		details=[]
		msg=""
		salesman_id = salesman["salesman_id"]
		latitude = salesman["latitude"]
		longitude = salesman["longitude"]
		last_update_id = salesman["last_update_id"]
		salesman_selfy_image = salesman["salesman_selfy_image"]
		start_time = salesman["start_time"]
		retailer_id = salesman["retailer_id"]
		now = datetime.now()
		today_date = now.strftime("%Y-%m-%d %H:%M:%S")

		'''get_salesman_touchbas_query = ("""SELECT * FROM `salesman_touchbase` WHERE `retailer_id` = %s and `salesman_id` = %s""")
		get_salesman_touchbase_data = (retailer_id,salesman_id)
		salesman_touchbase_data_count = cursor.execute(get_salesman_touchbas_query,get_salesman_touchbase_data)
		if salesman_touchbase_data_count > 0:
			salesman_touchbase_data = cursor.fetchone()
			salesman['salesman_touchbase_id'] = salesman_touchbase_data['salesman_touchbase_id']
			update_query = ("""UPDATE `salesman_touchbase` SET `salesman_id` = %s,`latitude` = %s,`longitude` = %s,`last_update_id` = %s,`salesman_selfy_image` = %s WHERE `retailer_id` = %s and `salesman_id` = %s""")
			update_data = (salesman_id,latitude,longitude,last_update_id,salesman_selfy_image,retailer_id,salesman_id)
			updatesalesman=cursor.execute(update_query,update_data)
		else:'''
		insert_query = ("""INSERT INTO `salesman_touchbase`(`salesman_id`,`latitude`,`longitude`,`last_update_id`,`salesman_selfy_image`,`start_time`,`retailer_id`,`last_update_ts`) VALUES(%s,%s,%s,%s,%s,%s,%s,%s)""")
		data=(salesman_id,latitude,longitude,last_update_id,salesman_selfy_image,start_time,retailer_id,today_date)
		insertdata=cursor.execute(insert_query,data)
		salesman['salesman_touchbase_id'] = cursor.lastrowid	
		
		connection.commit()
		cursor.close()
		
		return ({"attributes": {
	    				"status_desc": "Salesman Touchbase",
	    				"status": "success"	    				
	    				},
	    				"responseList":salesman}), status.HTTP_200_OK

#----------------------------------Add-Sales-man-Touchbase------------------------------------------------#

#----------------------------------Salesman-Dashboard------------------------------------------------#

@name_space.route("/salesmanDashboard/<int:salesman_id>")	
class salesmanDashboard(Resource):
	def get(self,salesman_id):
		connection = bakesandcakes()
		cursor = connection.cursor()

		now = datetime.now()
		today_date = now.strftime("%Y-%m-%d")

		salesman_dashboard_data = {}


		get_retailer_count_query = ("""SELECT count(*) as retailer_count
			FROM `retailer_salesman_mapping` rsm
			INNER JOIN `retailer` r ON r.`retailer_id` = rsm.`retailer_id` 
			where `salesman_id` = %s""")
		get_retailer_count_data = (salesman_id)

		retailer_count = cursor.execute(get_retailer_count_query,get_retailer_count_data)

		if retailer_count > 0:
			retailer_count_data = cursor.fetchone()			
			salesman_dashboard_data['total_accounts'] = retailer_count_data['retailer_count']
		else:
			salesman_dashboard_data['total_accounts'] = 0

		get_retailer_active_count_query = ("""SELECT count(*) as activate_retailer_count
			FROM `retailer_salesman_mapping` rsm
			INNER JOIN `retailer` r ON r.`retailer_id` = rsm.`retailer_id` 
			where `salesman_id` = %s and r.`status` = 1""")
		get_retailer_active_count_data = (salesman_id)

		active_retailer_count = cursor.execute(get_retailer_active_count_query,get_retailer_active_count_data)

		if active_retailer_count > 0:
			active_retailer_count_data = cursor.fetchone()			
			salesman_dashboard_data['total_active_accounts'] = active_retailer_count_data['activate_retailer_count']
		else:
			salesman_dashboard_data['total_active_accounts'] = 0

		get_retailer_target_query = ("""SELECT * FROM `target` where `salesman_id` = %s and `target_type` = 1""")
		get_retailer_target_data = (salesman_id)
		retailer_target_count = cursor.execute(get_retailer_target_query,get_retailer_target_data)

		if retailer_target_count > 0:
			retailer_target_data = cursor.fetchone()
			salesman_dashboard_data['daily_run_rate'] = retailer_target_data['target_amount']
		else:
			salesman_dashboard_data['daily_run_rate'] = 0 

		get_salesman_achieve_target_query = ("""SELECT COALESCE(CAST(sum(`sales_amount`) as SIGNED),0) as total_sales_amount FROM `sales_order` where `sales_man_id` = %s  and `is_fullfilled` = 1 and `order_date` = %s and `order_status` = 1""")
		get_salesman_achieve_target_data = (salesman_id,today_date)
		salesman_achieve_target_count = cursor.execute(get_salesman_achieve_target_query,get_salesman_achieve_target_data)

		if salesman_achieve_target_count > 0:
			print(cursor._last_executed)
			salesman_achieve_target_data = cursor.fetchone()
			print(salesman_achieve_target_data)
			salesman_dashboard_data['asking_run_rate'] = salesman_achieve_target_data['total_sales_amount']
		else:
			salesman_dashboard_data['asking_run_rate'] = 0


		return ({"attributes": {
	    				"status_desc": "Salesman Touchbase",
	    				"status": "success"	    				
	    				},
	    				"responseList":salesman_dashboard_data}), status.HTTP_200_OK


#----------------------------------Salesman-Dashboard------------------------------------------------#

#----------------------------------retailer-List-By-Salesman-Id------------------------------------------------#

@name_space.route("/retailerListBySalesManId/<int:salesman_id>")	
class retailerListBySalesManId(Resource):
	def get(self,salesman_id):
		connection = bakesandcakes()
		cursor = connection.cursor()

		get_retailer_list_query = ("""SELECT r.`retailer_id`,r.`name`,r.`owner_name`,r.`logo`,r.`phoneno`,r.`whatsapp_no`,r.`email`,r.`location`,r.`latitude`,r.`longitude`,r.`comments`,r.`city`,r.`state`,r.`pincode`,r.`status`,r.`last_update_ts` FROM `retailer_salesman_mapping` rsm
									INNER JOIN `retailer` r ON r.`retailer_id` = rsm.`retailer_id` 
									WHERE `salesman_id` = %s""")
		get_retailer_list_data = (salesman_id)
		retailer_list_count = cursor.execute(get_retailer_list_query,get_retailer_list_data)

		if retailer_list_count > 0:
			retailer_list = cursor.fetchall()
			for key,data in enumerate(retailer_list):
				retailer_list[key]['last_update_ts'] = str(data['last_update_ts'])

				'''get_retailer_documents_query = ("""SELECT dt.`document_type_name`,rd.`document_link` 
													FROM `retailer_documents` rd
													INNER JOIN `document_type` dt ON dt.`document_type_id` = rd.`documents_type_id` 
													WHERE rd.`retailer_id` = %s""")
				get_retailer_documents_data = (data['retailer_id'])
				retailer_documents_count = cursor.execute(get_retailer_documents_query,get_retailer_documents_data)

				if retailer_documents_count > 0:
					retailer_documents_data = cursor.fetchall()
					retailer_list[key]['retailer_documents'] = retailer_documents_data
				else:
					retailer_list[key]['retailer_documents'] = []

				get_retailer_touchbase_query = ("""SELECT `latitude` as `retailer_touchbase_latitude`,`longitude` as `retailer_touchbase_longitutde`, `salesman_selfy_image` from `salesman_touchbase`
					WHERE `salesman_id` = %s and `retailer_id` = %s""")
				get_retailer_touchbase_data = (salesman_id,data['retailer_id'])
				retailer_touchbase_count = cursor.execute(get_retailer_touchbase_query,get_retailer_touchbase_data)

				if retailer_touchbase_count > 0:
					retailer_touchbase_data = cursor.fetchone()
					retailer_list[key]['retailer_touchbase_latitude'] = retailer_touchbase_data['retailer_touchbase_latitude']
					retailer_list[key]['retailer_touchbase_longitutde'] = retailer_touchbase_data['retailer_touchbase_longitutde']
					retailer_list[key]['salesman_selfy_image'] = retailer_touchbase_data['salesman_selfy_image']
				else:
					retailer_list[key]['retailer_touchbase_latitude'] = ""
					retailer_list[key]['retailer_touchbase_longitutde'] = ""
					retailer_list[key]['salesman_selfy_image'] = ""

				get_lifetime_value_query = ("""SELECT COALESCE(CAST(sum(`sales_amount`) as SIGNED),0) as lifetime_sales FROM `sales_order` where `sales_man_id` = %s and `retailer_id` = %s and `is_fullfilled` = 1""")
				get_lifetime_value_data = (salesman_id,data['retailer_id'])
				lifetime_value_query = cursor.execute(get_lifetime_value_query,get_lifetime_value_data)

				if lifetime_value_query > 0:
					lifetime_value_data = cursor.fetchone()
					retailer_list[key]['lifetime_value'] = lifetime_value_data['lifetime_sales']
				else:
					retailer_list[key]['lifetime_value'] = 0 

				get_payment_pending_value_query = ("""SELECT COALESCE(CAST(sum(`sales_amount`) as SIGNED),0) as payment_pending_value FROM `sales_order` where `sales_man_id` = %s and `retailer_id` = %s and `is_fullfilled` = 1 and `is_payment` = 0""")
				get_payment_pending_value_data = (salesman_id,data['retailer_id'])
				payment_pending_value_query = cursor.execute(get_payment_pending_value_query,get_payment_pending_value_data)

				if payment_pending_value_query > 0:
					payment_pending_value_data = cursor.fetchone()
					retailer_list[key]['payment_pending'] = payment_pending_value_data['payment_pending_value']
				else:
					retailer_list[key]['payment_pending'] = 0

				get_retailer_task_type_query = ("""SELECT `retailer_task_type_id`,`retailer_task_type` FROM `retailer_task_type`""")
				get_retailer_task_type_count = cursor.execute(get_retailer_task_type_query)
				if get_retailer_task_type_count > 0:
					task_list = cursor.fetchall()
					for tkey,tdata in enumerate(task_list):
						get_retailer_salesman_task_status_query = ("""SELECT * FROM `salesman_retailer_task` where `retailer_id` = %s and `salesman_id` = %s and `retailer_task_type_id` = %s""")
						get_retailer_salesman_task_status_data = (data['retailer_id'],salesman_id,tdata['retailer_task_type_id'])
						retailer_salesman_task_status_count = cursor.execute(get_retailer_salesman_task_status_query,get_retailer_salesman_task_status_data)

						if retailer_salesman_task_status_count > 0:
							task_list[tkey]['is_complete'] = 1
						else:
							task_list[tkey]['is_complete'] = 0

					retailer_list[key]['task_list'] = task_list
				else:
					retailer_list[key]['task_list'] = []

				retailer_list[key]['lmtd'] = 12000
				retailer_list[key]['mtd'] = 22000				
				retailer_list[key]['credit'] = 10000
				retailer_list[key]['product_purchase'] = 40'''
		else:
			retailer_list = []

		return ({"attributes": {
	    				"status_desc": "Retailer List",
	    				"status": "success"	    				
	    				},
	    				"responseList":retailer_list}), status.HTTP_200_OK


#----------------------------------retailer-List-By-Salesman-Id------------------------------------------------#

#----------------------------------retailer-Details-By-Retailer-Id------------------------------------------------#

@name_space.route("/retailerDetailsByRetailerId/<int:retailer_id>/<int:touchbase_id>")	
class retailerDetailsByRetailerId(Resource):
	def get(self,retailer_id,touchbase_id):
		connection = bakesandcakes()
		cursor = connection.cursor()

		get_retailer_query = ("""SELECT r.`retailer_id`,r.`name`,r.`owner_name`,r.`logo`,r.`phoneno`,r.`whatsapp_no`,r.`email`,r.`location`,r.`latitude`,r.`longitude`,r.`comments`,r.`city`,r.`state`,r.`pincode`,r.`status`,r.`last_update_ts`,rsm.`salesman_id` 
									FROM `retailer_salesman_mapping` rsm
									INNER JOIN `retailer` r ON r.`retailer_id` = rsm.`retailer_id` 
									WHERE r.`retailer_id` = %s""")
		get_retailer_data = (retailer_id)
		retailer_data_count = cursor.execute(get_retailer_query,get_retailer_data)

		if retailer_data_count > 0:
			retailer_data = cursor.fetchone()
			retailer_data['last_update_ts'] = str(retailer_data['last_update_ts'])

			get_retailer_documents_query = ("""SELECT dt.`document_type_name`,rd.`document_link` 
													FROM `retailer_documents` rd
													INNER JOIN `document_type` dt ON dt.`document_type_id` = rd.`documents_type_id` 
													WHERE rd.`retailer_id` = %s""")
			get_retailer_documents_data = (retailer_data['retailer_id'])
			retailer_documents_count = cursor.execute(get_retailer_documents_query,get_retailer_documents_data)

			if retailer_documents_count > 0:
				retailer_documents_data = cursor.fetchall()
				retailer_data['retailer_documents'] = retailer_documents_data
			else:
				retailer_data['retailer_documents'] = []

			get_retailer_touchbase_query = ("""SELECT `salesman_touchbase_id`,`latitude` as `retailer_touchbase_latitude`,`longitude` as `retailer_touchbase_longitutde`, `salesman_selfy_image`,`start_time`,`end_time`,`total_time_spent` from `salesman_touchbase`
					WHERE `salesman_id` = %s and `retailer_id` = %s and `salesman_touchbase_id` = %s""")
			get_retailer_touchbase_data = (retailer_data['salesman_id'],retailer_data['retailer_id'],touchbase_id)
			retailer_touchbase_count = cursor.execute(get_retailer_touchbase_query,get_retailer_touchbase_data)

			if retailer_touchbase_count > 0:
				retailer_touchbase_data = cursor.fetchone()
				retailer_data['salesman_touchbase_id'] = retailer_touchbase_data['salesman_touchbase_id']
				retailer_data['retailer_touchbase_latitude'] = retailer_touchbase_data['retailer_touchbase_latitude']
				retailer_data['retailer_touchbase_longitutde'] = retailer_touchbase_data['retailer_touchbase_longitutde']
				retailer_data['salesman_selfy_image'] = retailer_touchbase_data['salesman_selfy_image']
				retailer_data['start_time'] = str(retailer_touchbase_data['start_time'])
				retailer_data['end_time'] = str(retailer_touchbase_data['end_time'])
				retailer_data['total_time_spent'] = str(retailer_touchbase_data['total_time_spent'])
			else:
				retailer_data['salesman_touchbase_id'] = 0
				retailer_data['retailer_touchbase_latitude'] = ""
				retailer_data['retailer_touchbase_longitutde'] = ""
				retailer_data['salesman_selfy_image'] = ""
				retailer_data['start_time'] = ""
				retailer_data['end_time'] = ""
				retailer_data['total_time_spent'] = ""

			get_lifetime_value_query = ("""SELECT COALESCE(CAST(sum(`sales_amount`) as SIGNED),0) as lifetime_sales FROM `sales_order` where `sales_man_id` = %s and `retailer_id` = %s and `order_status` = 1""")
			get_lifetime_value_data = (retailer_data['salesman_id'],retailer_data['retailer_id'])
			lifetime_value_query = cursor.execute(get_lifetime_value_query,get_lifetime_value_data)

			if lifetime_value_query > 0:
				lifetime_value_data = cursor.fetchone()
				retailer_data['lifetime_value'] = lifetime_value_data['lifetime_sales']
			else:
				retailer_data['lifetime_value'] = 0 

			get_payment_pending_value_query = ("""SELECT COALESCE(CAST(sum(`sales_amount`) as SIGNED),0) as payment_pending_value FROM `sales_order` where `sales_man_id` = %s and `retailer_id` = %s and `is_fullfilled` = 1 and `is_payment` = 0 and `order_status` = 1""")
			get_payment_pending_value_data = (retailer_data['salesman_id'],retailer_data['retailer_id'])
			payment_pending_value_query = cursor.execute(get_payment_pending_value_query,get_payment_pending_value_data)

			if payment_pending_value_query > 0:
				payment_pending_value_data = cursor.fetchone()
				retailer_data['payment_pending'] = payment_pending_value_data['payment_pending_value']
			else:
				retailer_data['payment_pending'] = 0

			get_retailer_task_type_query = ("""SELECT `retailer_task_type_id`,`retailer_task_type` FROM `retailer_task_type`""")
			get_retailer_task_type_count = cursor.execute(get_retailer_task_type_query)
			if get_retailer_task_type_count > 0:
				task_list = cursor.fetchall()
				for tkey,tdata in enumerate(task_list):
					get_retailer_salesman_task_status_query = ("""SELECT `is_complete` FROM `salesman_retailer_task` where `retailer_id` = %s and `salesman_id` = %s and `retailer_task_type_id` = %s and `touchbase_id` = %s""")
					get_retailer_salesman_task_status_data = (retailer_data['retailer_id'],retailer_data['salesman_id'],tdata['retailer_task_type_id'],touchbase_id)
					retailer_salesman_task_status_count = cursor.execute(get_retailer_salesman_task_status_query,get_retailer_salesman_task_status_data)

					if retailer_salesman_task_status_count > 0:
						retailer_salesman_task_status_data = cursor.fetchone()
						task_list[tkey]['is_complete'] = retailer_salesman_task_status_data['is_complete']
					else:
						task_list[tkey]['is_complete'] = 0

				retailer_data['task_list'] = task_list
			else:
				retailer_data['task_list'] = []

			get_retailer_product_purchase_query = ("""SELECT count(DISTINCT (p.`product_name`)) as  product_count
													FROM `sales_order_product` sop
													INNER JOIN `product` p ON p.`product_id` = sop.`product_id` 
													WHERE sop.`retailer_id` = %s and sop.`salesman_id` = %s""")
			get_retailer_product_purchase_data = (retailer_data['retailer_id'],retailer_data['salesman_id'])
			retailer_product_purchase_count = cursor.execute(get_retailer_product_purchase_query,get_retailer_product_purchase_data)

			if retailer_product_purchase_count > 0:
				retailer_product_purchase_data = cursor.fetchone()
				retailer_data['product_purchase'] = retailer_product_purchase_data['product_count']
			else:
				retailer_data['product_purchase'] = 0

			get_retailer_sales_order_count_query = ("""SELECT count(*) as  sales_order_count
													FROM `sales_order` so
													WHERE so.`retailer_id` = %s and so.`sales_man_id` = %s and so.`order_status` = 1""")
			get_retailer_sales_order_count_data = (retailer_data['retailer_id'],retailer_data['salesman_id'])
			count_retailer_sales_order_count = cursor.execute(get_retailer_sales_order_count_query,get_retailer_sales_order_count_data)

			if count_retailer_sales_order_count > 0:
				retailer_sales_order_count_data = cursor.fetchone()
				retailer_data['sales_order_count'] = retailer_sales_order_count_data['sales_order_count']
			else:
				retailer_data['sales_order_count'] = 0

			today = date.today()
			now = datetime.now()	
			mtd_end_date = now.strftime("%Y-%m-%d")
			day = '01'
			mtd_start_date = today.replace(day=int(day))
			

			get_mtd_value_query = ("""SELECT COALESCE(CAST(sum(`sales_amount`) as SIGNED),0) as mtd_sales FROM `sales_order` where `sales_man_id` = %s and `retailer_id` = %s and  date(`order_date`) >= %s and date(`order_date`) <= %s and `order_status` = 1""")
			get_mtd_value_data = (retailer_data['salesman_id'],retailer_data['retailer_id'],mtd_start_date,mtd_end_date)
			mtd_value_query = cursor.execute(get_mtd_value_query,get_mtd_value_data)

			if mtd_value_query > 0:
				mtd_value_data = cursor.fetchone()
				retailer_data['mtd'] = mtd_value_data['mtd_sales']
			else:
				retailer_data['mtd'] = 0 

			last_day_of_prev_month = date.today().replace(day=1) - timedelta(days=1)
			start_day_of_prev_month = date.today().replace(day=1) - timedelta(days=last_day_of_prev_month.day)	
			lmtd_start_date = start_day_of_prev_month.strftime("%Y-%m-%d")				
			lmtd_end_date = last_day_of_prev_month.strftime("%Y-%m-"+str(today.day))

			get_lmtd_value_query = ("""SELECT COALESCE(CAST(sum(`sales_amount`) as SIGNED),0) as lmtd_sales FROM `sales_order` where `sales_man_id` = %s and `retailer_id` = %s and  date(`order_date`) >= %s and date(`order_date`) <= %s and `order_status` = 1""")
			get_lmtd_value_data = (retailer_data['salesman_id'],retailer_data['retailer_id'],lmtd_start_date,lmtd_end_date)
			lmtd_value_query = cursor.execute(get_lmtd_value_query,get_lmtd_value_data)

			if lmtd_value_query > 0:
				lmtd_value_data = cursor.fetchone()
				retailer_data['lmtd'] = lmtd_value_data['lmtd_sales']
			else:
				retailer_data['lmtd'] = 0 

			retailer_data['credit'] = 0

			get_total_order_query = ("""SELECT COALESCE(CAST(sum(`payment_amount`) as SIGNED),0) as total_order FROM `payment_transaction` where `salesman_id` = %s and `retailer_id` = %s and `payment_type` = 1""")
			get_total_order_data = (retailer_data['salesman_id'],retailer_data['retailer_id'])
			get_total_order_count =  cursor.execute(get_total_order_query,get_total_order_data)
			if get_total_order_count > 0:
				total_order_data = cursor.fetchone()
				total_order = total_order_data['total_order']
			else:
				total_order = 0

			get_total_return_query = ("""SELECT COALESCE(CAST(sum(`payment_amount`) as SIGNED),0) as total_return FROM `payment_transaction` where `salesman_id` = %s and `retailer_id` = %s and `payment_type` = 2""")
			get_total_return_data = (retailer_data['salesman_id'],retailer_data['retailer_id'])
			get_total_return_count =  cursor.execute(get_total_return_query,get_total_return_data)
			if get_total_return_count > 0:
				total_return_data = cursor.fetchone()
				total_return = total_return_data['total_return']
			else:
				total_return = 0

			get_total_cash_collected_query = ("""SELECT COALESCE(CAST(sum(`payment_amount`) as SIGNED),0) as total_cash_collected FROM `payment_transaction` where `salesman_id` = %s and `retailer_id` = %s and `payment_type` = 3""")
			get_total_cash_collected_data = (retailer_data['salesman_id'],retailer_data['retailer_id'])
			get_total_cash_collected_count =  cursor.execute(get_total_cash_collected_query,get_total_cash_collected_data)
			if get_total_cash_collected_count > 0:
				total_cash_collected_data = cursor.fetchone()
				total_cash_collected = total_cash_collected_data['total_cash_collected']
			else:
				total_cash_collected = 0

			due_amount = total_order - (total_return+total_cash_collected)

			retailer_data['due_amount'] = due_amount

		else:
			retailer_data = {}

		return ({"attributes": {
	    				"status_desc": "Retailer Data",	
	    				"status": "success"	    				
	    				},
	    				"responseList":retailer_data}), status.HTTP_200_OK

#----------------------------------Product-Purchase-List-By-Retailer------------------------------------------------#

@name_space.route("/productPurchaseListByRetailer/<int:retailer_id>/<int:salesman_id>")	
class productPurchaseListByRetailer(Resource):
	def get(self,retailer_id,salesman_id):
		connection = bakesandcakes()
		cursor = connection.cursor()

		get_retailer_product_purchase_query = ("""SELECT distinct p.`product_name` 
													FROM `sales_order_product` sop
													INNER JOIN `product` p ON p.`product_id` = sop.`product_id` 
													WHERE sop.`retailer_id` = %s and sop.`salesman_id` = %s""")
		get_retailer_product_purchase_data = (retailer_id,salesman_id)
		retailer_product_purchase_count = cursor.execute(get_retailer_product_purchase_query,get_retailer_product_purchase_data)

		if retailer_product_purchase_count > 0:
			product_list = cursor.fetchall()
		else:
			product_list = []

		return ({"attributes": {
	    				"status_desc": "Product List",
	    				"status": "success"	    				
	    				},
	    				"responseList":product_list}), status.HTTP_200_OK


#----------------------------------Product-Purchase-List-By-Retailer------------------------------------------------#

#----------------------------------retailer-List-By-Geo-Location------------------------------------------------#

@name_space.route("/retailerListByLatLong/<string:latitute>/<string:logitute>/<int:salesman_id>")	
class retailerListByLatLong(Resource):
	def get(self,latitute,logitute,salesman_id):
		connection = bakesandcakes()
		cursor = connection.cursor()

		'''loc1=(28.426846,77.088834)
		loc2=(28.394231,77.050308)
		km = hs.haversine(loc1,loc2)

		meter = km * 1000.0
		print(meter)'''

		new_retailer_list = []

		get_retailer_list_query = ("""SELECT r.`retailer_id`,r.`name`,r.`owner_name`,r.`logo`,r.`phoneno`,r.`whatsapp_no`,r.`email`,r.`location`,r.`latitude`,r.`longitude`,r.`comments`,r.`city`,r.`state`,r.`pincode`,r.`status`,r.`last_update_ts` FROM `retailer_salesman_mapping` rsm
									INNER JOIN `retailer` r ON r.`retailer_id` = rsm.`retailer_id` 
									WHERE `salesman_id` = %s""")
		get_retailer_list_data = (salesman_id)
		retailer_list_count = cursor.execute(get_retailer_list_query,get_retailer_list_data)

		if retailer_list_count > 0:
			retailer_list = cursor.fetchall()
			for key,data in enumerate(retailer_list):

				#loc1 = (latitute,logitute)
				#loc2=(data['latitude'],data['longitude'])

				loc1=(float(latitute),float(logitute))
				#loc2=(28.426846,77.088834)
				loc2=(float(data['latitude']),float(data['longitude']))
				km = hs.haversine(loc1,loc2)

				meter = km * 1000.0
				print(meter)

				if meter <= 2000:
					print(data)
					new_retailer_list.append(retailer_list[key])

		for key,data in enumerate(new_retailer_list):
				new_retailer_list[key]['last_update_ts'] = str(data['last_update_ts'])



		return ({"attributes": {
	    				"status_desc": "Retailer Data",	
	    				"status": "success"	    				
	    				},
	    				"responseList":new_retailer_list}), status.HTTP_200_OK

				

#----------------------------------retailer-List-By-Geo-Location------------------------------------------------#



@name_space.route("/geolocationDifference/<string:latitute>/<string:logitute>")	
class geolocationDifference(Resource):
	def get(self,latitute,logitute):
		connection = bakesandcakes()
		cursor = connection.cursor()

		loc1=(float(latitute),float(logitute))
		loc2=(26.5175848,84.5906555)

		km = hs.haversine(loc1,loc2)
		meter = km * 1000.0
		print(meter)


		return ({"attributes": {
	    				"status_desc": "Retailer Data",	
	    				"status": "success"	    				
	    				},
	    				"responseList" : meter}), status.HTTP_200_OK


#----------------------Retailer-Check-Out---------------------#

@name_space.route("/retailerCheckOut/<int:retailer_id>/<int:salesman_id>/<int:touchbase_id>")
class retailerCheckOut(Resource):
	@api.expect(retailer_checkout_model)
	def put(self,retailer_id,salesman_id,touchbase_id):

		connection = bakesandcakes()
		cursor = connection.cursor()
		details = request.get_json()
		print(details)

		salesman_retailer_task_type = details['salesman_retailer_task_type']

		for key,data in enumerate(salesman_retailer_task_type):		
			
			get_salesman_retailer_task_query = ("""SELECT * FROM `salesman_retailer_task` where `retailer_id` = %s and `salesman_id` = %s and `retailer_task_type_id` = %s and `touchbase_id` = %s""")
			get_salesman_retailer_task_data = (retailer_id,salesman_id,data['task_id'],touchbase_id)
			salesman_retailer_task_count = cursor.execute(get_salesman_retailer_task_query,get_salesman_retailer_task_data)	

			if salesman_retailer_task_count == 0 :				
				insert_query = ("""INSERT INTO `salesman_retailer_task`(`retailer_task_type_id` ,`is_complete`, `salesman_id` , `retailer_id`,`touchbase_id`,`last_update_id`,`last_update_ts`) VALUES(%s,%s,%s,%s,%s,%s,CURRENT_TIMESTAMP())""")
				insert_data = (data['task_id'],data['status'],salesman_id,retailer_id,touchbase_id,salesman_id)
				cursor.execute(insert_query,insert_data)
			else:
				update_query = ("""UPDATE `salesman_retailer_task` SET `is_complete` = %s
							WHERE `retailer_id` = %s and `salesman_id` = %s and `touchbase_id` = %s and `retailer_task_type_id` = %s""")
				update_data = (data['status'],retailer_id,salesman_id,touchbase_id,data['task_id'])
				cursor.execute(update_query,update_data)

		note = details['note']

		if note != '':
			insert_query = ("""INSERT INTO `retailer_task_note`(`retailer_id` ,`touchbase_id`,`salesman_id` , `note`,`last_update_id`,`last_update_ts`) VALUES(%s,%s,%s,%s,%s,CURRENT_TIMESTAMP())""")
			insert_data = (retailer_id,touchbase_id,salesman_id,note,salesman_id)
			cursor.execute(insert_query,insert_data)

		end_time = details['touchbase_end_time']
		total_time_spent = details['total_time_spent']

		update_touchbase_query = ("""UPDATE `salesman_touchbase` SET `end_time` = %s, `total_time_spent` = %s
							WHERE `salesman_touchbase_id` = %s""")
		update_touchbase_data = (end_time,total_time_spent,touchbase_id)
		cursor.execute(update_touchbase_query,update_touchbase_data)
				

		connection.commit()
		cursor.close()

		return ({"attributes": {
				    		"status_desc": "Retailer Check Out",
				    		"status": "success"
				},
				"responseList":details}), status.HTTP_200_OK
					

#----------------------Retailer-Check-Out---------------------#

#---------------------------sales-Order-List-By-Retailer-And-Sales-Man-Id------------------------------------#

@name_space.route("/salesOrderListByRetailrIdAndSalesManId/<int:retailer_id>/<int:salesman_id>")	
class salesOrderListByRetailrIdAndSalesManId(Resource):
	def get(self,retailer_id,salesman_id):
		connection = bakesandcakes()
		cursor = connection.cursor()

		get_sales_order_query = ("""SELECT * FROM `sales_order` where `retailer_id` = %s and `sales_man_id` = %s order by `sales_order_id` desc""")
		get_sales_order_data = (retailer_id,salesman_id)
		sales_order_count = cursor.execute(get_sales_order_query,get_sales_order_data)

		if sales_order_count > 0:
			sales_order_data = cursor.fetchall()
			for key,data in enumerate(sales_order_data):
				sales_order_data[key]['order_date'] = str(data['order_date'])
				sales_order_data[key]['last_update_ts'] = str(data['last_update_ts'])

				
				if data['is_fullfilled'] == 2:
					get_sales_order_product_query = ("""SELECT sop.`product_id`,p.`product_name`,p.`in_price`,p.`out_price`,sop.`qty` 
						FROM `sales_order_product` sop 
						INNER JOIN `product` p ON p.`product_id` = sop.`product_id`
						where `sales_order_id` = %s""")
					get_sales_order_product_data = (data['sales_order_id'])
					sales_order_product_count = cursor.execute(get_sales_order_product_query,get_sales_order_product_data)
					if sales_order_product_count > 0:
						sales_order_product = cursor.fetchall()
						for sopkey,sopdata in enumerate(sales_order_product):
							get_partial_fullfill_product_query = ("""SELECT sum(`qty`) as total_qty FROM `partial_fullfill_product` where `sales_order_id` = %s and `product_id` = %s""")
							get_partial_fullfill_product_data = (data['sales_order_id'],sopdata['product_id'])
							get_partial_fullfill_product_count = cursor.execute(get_partial_fullfill_product_query,get_partial_fullfill_product_data)
							if get_partial_fullfill_product_count > 0:
								patial_fullfill_product_data = cursor.fetchone()
								sales_order_product[sopkey]['partial_fullfill_qty'] = int(patial_fullfill_product_data['total_qty'])
							else:
								sales_order_product[sopkey]['partial_fullfill_qty'] = 0

						sales_order_data[key]['order_product'] = sales_order_product
					else:
						sales_order_data[key]['order_product'] = []
				else:
					get_sales_order_product_query = ("""SELECT sop.`product_id`,p.`product_name`,p.`in_price`,p.`out_price`,sop.`qty` 
						FROM `sales_order_product` sop 
						INNER JOIN `product` p ON p.`product_id` = sop.`product_id`
						where `sales_order_id` = %s""")
					get_sales_order_product_data = (data['sales_order_id'])
					sales_order_product_count = cursor.execute(get_sales_order_product_query,get_sales_order_product_data)
					if sales_order_product_count > 0:
						sales_order_product = cursor.fetchall()
						sales_order_data[key]['order_product'] = sales_order_product
					else:
						sales_order_data[key]['order_product'] = []

		else:
			sales_order_data = []

		return ({"attributes": {
				    		"status_desc": "Sales Order",
				    		"status": "success"
				},
				"responseList":sales_order_data}), status.HTTP_200_OK


#---------------------------sales-Order-List-By-Retailer-And-Sales-Man-Id------------------------------------#		

#----------------------Update-Salesman-Task---------------------#

@name_space.route("/updateTask/<int:salesman_task_id>")
class updateTask(Resource):
	@api.expect(task_put_model)
	def put(self,salesman_task_id):

		connection = bakesandcakes()
		cursor = connection.cursor()
		details = request.get_json()

		is_complte = details['is_complte']

		update_query = ("""UPDATE `salesman_task` SET `is_complete` = %s
							WHERE `salesman_task_id` = %s""")
		update_data = (is_complte,salesman_task_id)
		cursor.execute(update_query,update_data)

		return ({"attributes": {
				    		"status_desc": "Salesman Task",
				    		"status": "success"
				},
				"responseList":details}), status.HTTP_200_OK

#----------------------Update-Salesman-Task---------------------#

#---------------------------get-Inventory-List-By-Sales-Order-----------------------------------#

@name_space.route("/getInventoryListBySalesOrder/<int:sales_order_id>")	
class getInventoryListBySalesOrder(Resource):
	def get(self,sales_order_id):
		connection = bakesandcakes()
		cursor = connection.cursor()

		get_inventory_list = ("""SELECT * FROM `inventory_master`""")
		get_inventory_count = cursor.execute(get_inventory_list)
		inventory_data = cursor.fetchall()

		for key,data in enumerate(inventory_data):
			inventory_data[key]['last_update_ts'] = str(data['last_update_ts'])

			get_sales_order_product_query = ("""SELECT sop.`product_id`,p.`product_name`,sop.`qty`,pim.`qty` as available_qty 
					FROM `sales_order_product` sop 
					INNER JOIN `product` p ON p.`product_id` = sop.`product_id`
					INNER JOIN `product_inventory_mapping` pim ON pim.`product_id` = sop.`product_id`
					where `sales_order_id` = %s and `inventory_id` = %s""")
			get_sales_order_product_data = (sales_order_id,data['inventory_id'])
			sales_order_product_count = cursor.execute(get_sales_order_product_query,get_sales_order_product_data)
			if sales_order_product_count > 0:
				sales_order_product = cursor.fetchall()
				inventory_data[key]['order_product'] = sales_order_product
			else:
				get_sales_order_product_query = ("""SELECT sop.`product_id`,p.`product_name`,sop.`qty`
					FROM `sales_order_product` sop 
					INNER JOIN `product` p ON p.`product_id` = sop.`product_id`
					where `sales_order_id` = %s""")
				get_sales_order_product_data = (sales_order_id)
				sales_order_product_count = cursor.execute(get_sales_order_product_query,get_sales_order_product_data)
				sales_order_product = cursor.fetchall()
				for sokey,sodata in enumerate(sales_order_product):
					sales_order_product[sokey]['available_qty'] = 0
				inventory_data[key]['order_product'] = sales_order_product


		return ({"attributes": {
				    		"status_desc": "Inventory",
				    		"status": "success"
				},
				"responseList":inventory_data}), status.HTTP_200_OK

#---------------------------get-Inventory-List-By-Sales-Order-----------------------------------#

#---------------------------get-Document-List_By_retailer-Id-----------------------------------#

@name_space.route("/getDocumentListByRetailerId/<int:retailer_id>")	
class getDocumentListByRetailerId(Resource):
	def get(self,retailer_id):
		connection = bakesandcakes()
		cursor = connection.cursor()

		get_document_list_query = ("""SELECT rd.*,dt.`document_type_name`
					FROM `retailer_documents` rd	
					INNER JOIN `document_type` dt ON dt.`document_type_id` = rd.`documents_type_id`				
					where `retailer_id` = %s""")
		get_document_list_data = (retailer_id)
		count_document_list = cursor.execute(get_document_list_query,get_document_list_data)

		if count_document_list > 0:
			document_data =  cursor.fetchall()

			for key,data in enumerate(document_data):
				document_data[key]['last_update_ts'] = str(data['last_update_ts'])
		else:
			document_data = []

		return ({"attributes": {
				    		"status_desc": "Document List",
				    		"status": "success"
				},
				"responseList":document_data}), status.HTTP_200_OK



#---------------------------get-Document-List_By_retailer-Id-----------------------------------#

#----------------------Product-List---------------------#

@name_space.route("/ProductListByInventoryId/<int:inventory_id>/<int:retailer_id>/<int:salesman_id>")	
class ProductListByInventoryId(Resource):
	def get(self,inventory_id,retailer_id,salesman_id):
		connection = bakesandcakes()
		cursor = connection.cursor()

		get_product_type_list_query = ("""SELECT * FROM `product_type_master`""")
		product_type_list_count = cursor.execute(get_product_type_list_query)

		if product_type_list_count > 0:
			product_type_list_data = cursor.fetchall()

			for key,data in enumerate(product_type_list_data):
				product_type_list_data[key]['last_update_ts'] = str(data['last_update_ts'])
				get_product_list_query = ("""SELECT * FROM `product` where `product_type_id` = %s""")
				get_product_list_data = (data['product_type_id'])
				product_list_count = cursor.execute(get_product_list_query,get_product_list_data)
				if product_list_count > 0:
					product_list_data = cursor.fetchall()
					for pkey,pdata in enumerate(product_list_data):
						get_product_qty_by_inventory = ("""SELECT *
							FROM `product_inventory_mapping` where `inventory_id` = %s and `product_id` = %s""")
						get_product_qty_data = (inventory_id,pdata['product_id'])
						count_product_qty = cursor.execute(get_product_qty_by_inventory,get_product_qty_data)
						if count_product_qty > 0:
							product_inventoty_qty = cursor.fetchone()
							product_list_data[pkey]['qty'] = product_inventoty_qty['qty']
						else:
							product_list_data[pkey]['qty'] = 0

						get_product_retun_count_query = ("""SELECT sum(qty) as total_retun_product_count FROM `sales_order_product` where `product_id` = %s and `retailer_id` = %s and `salesman_id` = %s and `order_status` = 2""")
						get_product_retun_count_data = (pdata['product_id'],retailer_id,salesman_id)
						get_product_retun_count = cursor.execute(get_product_retun_count_query,get_product_retun_count_data)

						if get_product_retun_count > 0:
							product_retun_data = cursor.fetchone()
							if product_retun_data['total_retun_product_count'] is None:
								product_list_data[pkey]['return_qty'] = 0
							else:	
								product_list_data[pkey]['return_qty'] = int(product_retun_data['total_retun_product_count'])
						else:
							product_list_data[pkey]['return_qty'] = 0

						product_list_data[pkey]['last_update_ts'] = str(pdata['last_update_ts'])
				else:
					product_list_data = []

				product_type_list_data[key]['product_list'] = product_list_data		
				

		else:
			product_type_list_data = []

		return ({"attributes": {
				    		"status_desc": "Type List",
				    		"status": "success"
				},
				"responseList":product_type_list_data}), status.HTTP_200_OK
