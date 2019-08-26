from lxml import etree
from lxml import objectify
import requests as rq
from pprint import pprint
import pandas as pd
import os
import calendar
import time

import pendulum
import collections
import datetime
import logging
import time
import uuid


NS = {
		"SOAP-ENV": "http://schemas.xmlsoap.org/soap/envelope/",
		"sushi": "http://www.niso.org/schemas/sushi",
		"sushicounter": "http://www.niso.org/schemas/sushi/counter",
		"counter": "http://www.niso.org/schemas/counter",
	}


def add_sub(parent, tag, content):
	temp = etree.SubElement(parent, tag)
	temp.text = content
	return temp

def spaceout(n = 2):
	for i in range(n):
		print()
#
def view(parent_node):
	print("************************************************************")
	print(etree.tostring(parent_node,pretty_print = True, encoding = 'unicode')) 		#look at the whole tree
	print("************************************************************")

#############################################################################################################################
#############################################################################################################################
#
#############################################################################################################################
def raw_xml_rep( lg, mnth = 3, yr = 2019, report = "JR1", release = 4):
	'''
	#Retrive the XML-formatted report from the given SUSHI server
	lg -> a 3-member tuple of strings that holds the SUSHI server's location and login credentials
		lg[0] -> (str) The SUSHI server URL
		lg[1] -> (str) The SUSHI requestor ID
		lg[2] -> (str) the SUSHI customer ID
	mnth -> (int) the month of the requested report
	yr -> (int) the year of the requested report
	report -> (str) the type of report requested
	release -> (int) the COUNTER release of the requested report. Currently defaults to 4.
	'''
	wsdl_url = lg[0]
	start_date = datetime.date(yr,mnth,1)
	end_date = datetime.date(yr, mnth, calendar.monthrange(yr,mnth)[1])
	requestor_id = lg[1]
	requestor_email=None
	requestor_name=None
	customer_reference=lg[2]
	customer_name=None
	#report="JR2" 		#tombstoned for testing
	sushi_dump=False
	verify=True
	#release = 4		#tombstoned for testing

	#========================================================================
	#INITIALIZE VARIABLES AND CONTEXT VARIABLES
	NS = {
		"SOAP-ENV": "http://schemas.xmlsoap.org/soap/envelope/",
		"sushi": "http://www.niso.org/schemas/sushi",
		"sushicounter": "http://www.niso.org/schemas/sushi/counter",
		"counter": "http://www.niso.org/schemas/counter",
	}
	#=======================================================================
	rooty = etree.Element("{%(SOAP-ENV)s}Envelope" % NS, nsmap=NS)	#This is the root of the tree that we're going to pass as the header to the SUSHI server.
	body = etree.SubElement(rooty, "{%(SOAP-ENV)s}Body" % NS)		#build the body node for the SUSHI request tree
	timestamp = pendulum.now("UTC").isoformat()						#Timestamp the report request
	rr = etree.SubElement(
		body,
		"{%(sushicounter)s}ReportRequest" % NS,
		{"Created": timestamp, "ID": str(uuid.uuid4())},
	)
	#=======================================================
	
	#Create the XML outline that's going to be submitted with the POST request to the SUSHI server for population.

	req = etree.SubElement(rr, "{%(sushi)s}Requestor" % NS)	# Link to the sushi schema and add this link into the etree with the 'Requestor' tag
	rid = add_sub(req, "{%(sushi)s}ID" % NS, requestor_id)	# Create a new subelement of 'req' tagged 'ID' containing the requestor ID
	#-----------
	req_name_element = add_sub(req, "{%(sushi)s}Name" % NS, requestor_name)		#Create a child node of 'req' tagged 'Name', holding the requestor name (Institution name)
	req_email_element = add_sub(req, "{%(sushi)s}Email" % NS, requestor_email)	#Create a child node of 'req' tagged 'Email', holding the requestor's provided email address
	#-----------
	cust_ref_elem = etree.SubElement(rr, "{%(sushi)s}CustomerReference" % NS)	#Create a child node of 'req' tagged 'CustomerReference'
	ci = add_sub(cust_ref_elem, "{%(sushi)s}ID" % NS, customer_reference)		#Create a child node of 'CustomerReference', tagged 'ID' and holding the cutomer ref ID

	cust_name_elem = add_sub(cust_ref_elem, "{%(sushi)s}Name" % NS, customer_name)

	report_def_elem = etree.SubElement(	  rr,   "{%(sushi)s}ReportDefinition" % NS,   Name=report,   Release=str(release)  )
	filters = etree.SubElement(report_def_elem, "{%(sushi)s}Filters" % NS)
	udr = etree.SubElement(filters, "{%(sushi)s}UsageDateRange" % NS)
	beg = etree.SubElement(udr, "{%(sushi)s}Begin" % NS)

	beg.text = start_date.strftime("%Y-%m-%d")
	end = etree.SubElement(udr, "{%(sushi)s}End" % NS)
	end.text = end_date.strftime("%Y-%m-%d")
	#print(etree.tostring(rooty,  pretty_print = True,  encoding = 'unicode'))
	payload = etree.tostring(   rooty,   pretty_print=True,   xml_declaration=True,   encoding="utf-8" )
	#=============================================================
	headers = {
		"SOAPAction": '"SushiService:GetReportIn"',
		"Content-Type": "text/xml; charset=UTF-8",
		"Content-Length": str(len(payload)),
	}
	
	response = rq.post(url=wsdl_url, headers=headers, data=payload, verify=verify) # Post the SUSHI tree to the server and save teh response as 'response'
	if sushi_dump:
		logger.debug(
			"SUSHI DUMP: request: %s \n\n response: %s", payload, response.content
		)
	#rt = etree.fromstring(response.content) # tombstoned for testing
	return response.content
#############################################################################################################################
#############################################################################################################################
#
#############################################################################################################################
def jr1_df(lgn, month, year):
	'''
	Pull down a single month's JR1 report, and return it as a DataFrame
	-----------------------------------------------------------------------------
	lgn -> a 3-member tuple of strings that holds the SUSHI server's location and login credentials
		lgn[0] -> (str) The SUSHI server URL
		lgn[1] -> (str) The SUSHI requestor ID
		lgn[2] -> (str) the SUSHI customer ID
	month -> (int) the month of the requested report
	year -> (int) the year of the requested report
	'''
	try:
		repo = etree.fromstring(raw_xml_rep( lg = lgn, mnth = month, yr = year, report = 'JR1'))

		colset = ['ItemName', 'Print_ISSN', 'Proprietary', 'Online_ISSN', 'ItemPlatform', 'ItemPublisher', 'ItemDataType', 'ft_total', 'ft_html', 'ft_pdf']
		#============================================================
		rootp = repo[0][0][3][0][1]
		bdf = pd.DataFrame(columns = colset)

		for i in range(2,len(rootp.getchildren())): #loop through the subtrees that have content to go into the dataframe
			temp = [''] * len(colset)				#Create the holder list that will hold this row and go into the DF
			for child in rootp[i].getchildren():		#loop through the first level of each subtree
			
				# Grab the elements that are in the first level of the rootp tree after the root
				for colname in colset:	#loop through all of the columns and grab the first-level texts whose tag matches a column name
					if "{http://www.niso.org/schemas/counter}"+colname == child.tag:
						temp[colset.index(colname)] = child.text

				# Grab the elements that are under an 'ItemIdentifier' subtree by thier type
				if child.tag == '{http://www.niso.org/schemas/counter}ItemIdentifier':	#look at each subtree whose root is an ItemIdentifier
					ItemID_children_list = list(child.getchildren())	#This should be two elements: the type and the value
					type = str(child.findall('{http://www.niso.org/schemas/counter}Type')[0].text)	#grab the type, which should match a column name
					val = str(child.findall('{http://www.niso.org/schemas/counter}Value')[0].text)		#grab the value, in a sting format.
					if type in colset:	#make sure the type is an element in colset
						temp[colset.index(type)] = val	#add the new val to temp in the correct column
				
				# Grab the elements that are under an 'ItemPerformance' tag.
				if child.tag == '{http://www.niso.org/schemas/counter}ItemPerformance':
					instance = child.find('{http://www.niso.org/schemas/counter}Instance')
					metric_type = instance.find('{http://www.niso.org/schemas/counter}MetricType').text
					ct = instance.find('{http://www.niso.org/schemas/counter}Count').text
					temp[colset.index(metric_type)] = ct
			bdf.loc[len(bdf)] = temp
		return bdf
	# Catch potential errors and return an error flag and an empty dataframe. This matches what's sometimes
	 # returned from error-ed out SUSHI requests, so we'll just check for this format of dataframe to determine when an error has occurred
	except:
		print("Error in handling",lgn)
		err_df = pd.DataFrame(columns = colset)
		return err_df
#############################################################################################################################
#############################################################################################################################
#
#############################################################################################################################
def mr1_df(lgn,month,year):
	'''
	Pull down a single month's MR1 report, and return it as a DataFrame
	-----------------------------------------------------------------------------
	lgn -> a 3-member tuple of strings that holds the SUSHI server's location and login credentials
		lgn[0] -> (str) The SUSHI server URL
		lgn[1] -> (str) The SUSHI requestor ID
		lgn[2] -> (str) the SUSHI customer ID
	month -> (int) the month of the requested report
	year -> (int) the year of the requested report
	'''
	# Initialize the report in var 'repo'
	repo = etree.fromstring(raw_xml_rep( lg = lgn, mnth = month, yr = year, report = 'MR1'))
	month_date = pd.Period(freq = 'M', year = year , month = month)


	# Initialize the colset, df, and report data parent node
	colset = ['ItemName', 'ItemPlatform', 'ItemPublisher', 'Category', 'MetricType', 'Date', 'Count']
	df = pd.DataFrame(columns = colset)
	try:
		rootp = repo[0][0][3][0][1] #	set up the root node for the actual report content
		
		for report_item in rootp.getchildren(): #	repo[0][0][3][0][1] is the parent node to all of the ReportItem nodes
			temp = [''] * len(colset)				#	Create the holder list that will hold this row and go into the DF 
			if report_item.tag == '{http://www.niso.org/schemas/counter}ReportItems':
				for data_node in report_item.getchildren():
					
					# Loop through all of the columns and grab the first-level texts whose tag matches a column name
					for colname in colset:
						if "{http://www.niso.org/schemas/counter}"+colname == data_node.tag:
							temp[colset.index(colname)] = data_node.text
					
					# Dig into the data_node's ItemPerformance subnode and grab the category, metric type, and count data points
					if data_node.tag == '{http://www.niso.org/schemas/counter}ItemPerformance':
						
						# Grab the top-level category data point
						categ = instance = data_node.find('{http://www.niso.org/schemas/counter}Category')
						temp[colset.index('Category')] = categ.text
						
						# Drill into the Instance node and find the 'MetricType' and 'Count' nodes, then add them to 'temp'
						instance = data_node.find('{http://www.niso.org/schemas/counter}Instance')
						metric_type = instance.find('{http://www.niso.org/schemas/counter}MetricType').text
						ct = instance.find('{http://www.niso.org/schemas/counter}Count').text
						temp[colset.index('MetricType')] = metric_type
						temp[colset.index('Date')] = month_date.strftime('%b-%Y')
						temp[colset.index('Count')] = ct
						
			# Add the completed row to the dataframe
			if temp != [''] * len(colset):
				df.loc[len(df)] = temp
		return df
	except:
		return df

#############################################################################################################################
#############################################################################################################################
#
#############################################################################################################################
def br1_df(lgn,month,year):
	# download report, define dataframe to populate and return, and format date for date column
	repo = etree.fromstring(raw_xml_rep( lg = lgn, mnth = month, yr = year, report = 'BR1'))
	colset = ['ItemName', 'ItemPlatform', 'ItemPublisher', 'Print_ISBN',  'Online_ISBN',  'Proprietary', 'ItemDataType', 'Category', 'MetricType', 'Date', 'Count']
	df = pd.DataFrame(columns = colset)
	date = str(month) + '/' + str(year)
	
	try:
		rootp = repo[0][0][3][0][1]
		for report_row_node in rootp:
			temp = [''] * len(colset)
			if report_row_node.tag == '{http://www.niso.org/schemas/counter}ReportItems':
				# Create a line-holder vector, then go into the actual data nodes for this row
				temp[colset.index('Date')] = date
				for data_node in report_row_node:
				
					 # STEP I: Loop through all of the columns and grab the first-level texts whose tag matches a column name
					 #NOTE: Might be better to replace with a parser to remove URL from XML tag and use "if-in" to check colset for remaining tag-text. 
					for colname in colset:
						if "{http://www.niso.org/schemas/counter}"+colname == data_node.tag:
							temp[colset.index(colname)] = data_node.text
							
					# STEP II: in each of the 3 ItemIdentifier nodes, grab the Type and Values subnodes' texts and 
					 #use the Type to identify the correct column for the Value text.
					if data_node.tag == '{http://www.niso.org/schemas/counter}ItemIdentifier':
						type = data_node.find('{http://www.niso.org/schemas/counter}Type').text
						value = data_node.find('{http://www.niso.org/schemas/counter}Value').text
						temp[colset.index(type)] = value
						
					#STEP III: grab the Category subnode's text and populate that column, then
					 #search the Instance subnode and take the values of its MetricType and Count subnodes.
					 #Use these values to populate the appropriate spaces in the array.
					if data_node.tag == '{http://www.niso.org/schemas/counter}ItemPerformance':
						temp[colset.index('Category')] = data_node.find('{http://www.niso.org/schemas/counter}Category').text
						instance = data_node.find('{http://www.niso.org/schemas/counter}Instance')
						temp[colset.index('MetricType')] = instance.find('{http://www.niso.org/schemas/counter}MetricType').text
						temp[colset.index('Count')] = instance.find('{http://www.niso.org/schemas/counter}Count').text

			# Add the completed row to the dataframe
			if temp != [''] * len(colset):
				df.loc[len(df)] = temp
		return df
	except:
		return df

#############################################################################################################################
#############################################################################################################################
#
#############################################################################################################################
def mr1_over_time(start_year, end_year, start_month, end_month, credentials, out_file = ''):
	'''
	This is the in-progress version of the MR1 over-time report harvester.
	'''
	col_set = ['ItemName', 'ItemPlatform', 'ItemPublisher', 'Category', 'MetricType']
	temp = pd.DataFrame(columns = col_set)
	for y in range (start_year, end_year +1):		# Loop through the full year range
		for m in range(start_month, end_month +1):	# loop through the full month range
			if temp.empty:	#if this is the first 
				temp = mr1_df(credentials,m,y)
				if temp.empty:
					print('This B empty.', m, y)
				else:
					print(m,y, "confirmed")
			else:
				try:
					start = time.time()
					to_add = mr1_df(credentials,m,y)
					temp = pd.concat([temp,mr1_df(credentials,m,y)], ignore_index = True)
					end = time.time()
					print(end-start, m, y)
				except:
					print(m, y, 'not available')
	temp.fillna(0, inplace = True)
	if out_file != '':
		temp.to_csv(out_file)
	return temp