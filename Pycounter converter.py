import pycounter.sushi
import csv
import pandas as pd
import datetime
import calendar as cal
import os


def tsv_name_temp(rep, num):
	return rep+str(num)+'.tsv'
############################################################################################################################################################
def tsv_name_final(rep, num):
	return rep+str(num)+'.tsv'
############################################################################################################################################################
def print_bar():
'''for formatting test output, print a spacing bar to the console.'''
	for i in range(200):
		print('=', end = '')
	print('=')
############################################################################################################################################################
def emaps_report(year,month,logins, platform_name):
'''return a dataframe for the COUNTER-4 'JR1' report, for the given month and year.
	----------------------------------------------------------------------------------------
	logins --- the 'requests' information (in string form) for the user's account and platform's SUSHI server URL.
		example: logins = ('https://pubs.acs.org/api/soap/analytics/SushiServices','SUSHI_requestor_ID','SUSHI_customer_ID')
	platform_name = the string-formatted name of the plaform you want to pull down from.
		'Alexander Street Press'
'''
	#initialize date range, temporary report name(tsv_name_temp)
	date_range = (datetime.date(year,month,1), datetime.date(year,month,cal.monthrange(year,month)[1])) #use cal.monthrange to get numerical final day of month.
	temp_tsv = platform_name + '_counter_report_temp' + str(397) + '.tsv'
	final_tsv = platform_name + '_counter_report_final' + str(937) + '.tsv'
	
	#Use pycounter/SUSHI to pull report in XML form, and convert to unicode data.
	report = pycounter.sushi.get_report(wsdl_url = logins[0],start_date = date_range[0],
										end_date = date_range[1],requestor_id = logins[1],
										customer_reference = logins[2],release = 4)
	#
	report.write_tsv(temp_tsv) #write TSV to tsv_rep_name
	
	#open the written tsv in read-mode, open the new tsv to be written to(tsv_name_final)	
	with open(temp_tsv,'r', encoding = 'utf-8') as f, open(final_tsv,'w', encoding = 'utf-8') as f1:
		for i in range(7): #skip the first 7 lines and then write the rest out to the final file
			next(f)		
		for line in f: #write the remaining lines out to f1, the ofstream
			f1.write(line)
	os.remove(temp_tsv) #Delete the temp tsv.
	ret = pd.read_csv(final_tsv, sep = '\t')  #now read the new tsv into a dataframe
	return ret
############################################################################################################################################################
def emaps_DB1_report(year,month,logins, platform_name):
'''
	return a dataframe for the COUNTER-4 'DB1' report, for the given month and year.
	----------------------------------------------------------------------------------------
	logins --- the 'requests' information (in string form) for the user's account and platform's SUSHI server URL.
		example: logins = ('https://pubs.acs.org/api/soap/analytics/SushiServices','SUSHI_requestor_ID','SUSHI_customer_ID')
	platform_name = the string-formatted name of the plaform you want to pull down from.
		example: "American Chemical Society" would be 'ACS', but long-form names also work.
'''
	#initialize date range, temporary report name(tsv_name_temp)
	date_range = (datetime.date(year,month,1), datetime.date(year,month,cal.monthrange(year,month)[1])) #use cal.monthrange to get numerical final day of month.
	temp_tsv = platform_name + '_counter_report_temp' + str(397) + '.tsv'
	final_tsv = platform_name + '_counter_report_final' + str(937) + '.tsv'
	
	#Use pycounter/SUSHI to pull report
	report = pycounter.sushi.get_report(wsdl_url = logins[0],start_date = date_range[0],
										end_date = date_range[1],requestor_id = logins[1],
										customer_reference = logins[2],report = "DB1", release = 4)
	#
	report.write_tsv(temp_tsv) #write TSV to tsv_rep_name
	
	#open the written tsv in read-mode, open the new tsv to be written to(tsv_name_final)	
	with open(temp_tsv,'r', encoding = 'utf-8') as f, open(final_tsv,'w', encoding = 'utf-8') as f1:
		for i in range(7): #skip the first 7 lines and then write the rest out to the final file
			next(f)		
		for line in f: #write the remaining lines out to f1, the ofstream
			f1.write(line)
	os.remove(temp_tsv) #Delete the temp tsv.
	ret = pd.read_csv(final_tsv, sep = '\t')  #now read the new tsv into a dataframe
	return ret
############################################################################################################################################################
def over_time_report(year1,month1,year2,month2,logins, platform_name):
'''
	return a dataframe for the COUNTER-4 'JR1' report, from year1,month1 to year2,month2.
	----------------------------------------------------------------------------------------
	logins --- the 'requests' information (in string form) for the user's account and platform's SUSHI server URL.
		example: logins = ('https://pubs.acs.org/api/soap/analytics/SushiServices','SUSHI_requestor_ID','SUSHI_customer_ID')
	platform_name = the string-formatted name of the plaform you want to pull down from.
		example: "American Chemical Society" would be 'ACS', but long-form names also work.
	year1 - the start year for the report, e.g. 2016
	year2 - the end year of the report, e.g. 2019
	month1 - the numerical first month to include in the report, e.g. March = 3, November = 11
	month1 - the numerical last month to include in the report, e.g. April = 4 = 3, October = 10
'''
	#initialize date range, temporary report name(tsv_name_temp)
	date_range = ( datetime.date(year1, month1, 1 ),  datetime.date(year2, month2, cal.monthrange(year2,month2)[1] ) )
	temp_tsv = platform_name + 'COUNTERrep_temp' + 'mppltr76706' + '.tsv'
	final_tsv = platform_name + '_OverTime_final' + str(937) + '.tsv'
	
	#Use pycounter/SUSHI to pull report in XML form
	report = pycounter.sushi.get_report(wsdl_url = logins[0],start_date = date_range[0],
									end_date = date_range[1],requestor_id = logins[1],
									customer_reference = logins[2],release = 4)
	#
	report.write_tsv(temp_tsv) #write TSV to tsv_rep_name

	#open the written tsv in read-mode, open the new tsv to be written to(tsv_name_final)	
	with open(temp_tsv,'r', encoding = 'utf-8') as f, open(final_tsv,'w', encoding = 'utf-8') as f1:
		for i in range(7): #skip the first 7 lines and then write the rest out to the final file
			next(f)		
		for line in f: #write the remaining lines out to f1, the ofstream
			f1.write(line)
	os.remove(temp_tsv) #Delete the temp tsv.
	ret = pd.read_csv(final_tsv, sep = '\t')  #now read the new tsv into a dataframe
	return ret
############################################################################################################################################################
def over_time_DB1_report(year1,month1,year2,month2,logins, platform_name):
'''
	Return a dataframe for the COUNTER-4 'DB1' report, from year1,month1 to year2,month2.
	----------------------------------------------------------------------------------------
	logins --- the 'requests' information (in string form) for the user's account and platform's SUSHI server URL.
		example: logins = ('https://pubs.acs.org/api/soap/analytics/SushiServices','SUSHI_requestor_ID','SUSHI_customer_ID')
	platform_name = the string-formatted name of the plaform you want to pull down from.
		example: "American Chemical Society" would be 'ACS', but long-form names also work.
	year1 - the start year for the report, e.g. 2016
	year2 - the end year of the report, e.g. 2019
	month1 - the numerical first month to include in the report, e.g. March = 3, November = 11
	month1 - the numerical last month to include in the report, e.g. April = 4 = 3, October = 10
'''
	#initialize date range, temporary report name(tsv_name_temp)
	date_range = ( datetime.date(year1, month1, 1 ),  datetime.date(year2, month2, cal.monthrange(year2,month2)[1] ) )
	temp_tsv = platform_name + 'COUNTERrep_temp' + 'mppltr76706' + '.tsv'
	final_tsv = platform_name + '_OverTime_final' + str(937) + '.tsv'
	
	#Use pycounter/SUSHI to pull report
	report = pycounter.sushi.get_report(wsdl_url = logins[0],start_date = date_range[0],
									end_date = date_range[1],requestor_id = logins[1],
									customer_reference = logins[2],report = "DB1", release = 4)
	#
	report.write_tsv(temp_tsv) #write TSV to tsv_rep_name

	#open the written tsv in read-mode, open the new tsv to be written to(tsv_name_final)	
	with open(temp_tsv,'r', encoding = 'utf-8') as f, open(final_tsv,'w', encoding = 'utf-8') as f1:
		for i in range(7): #skip the first 7 lines and then write the rest out to the final file
			next(f)		
		for line in f: #write the remaining lines out to f1, the ofstream
			f1.write(line)
	os.remove(temp_tsv) #Delete the temp tsv.
	ret = pd.read_csv(final_tsv, sep = '\t')  #now read the new tsv into a dataframe
	return ret
