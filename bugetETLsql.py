'''
Created on Dec 12, 2013

@author: jay
'''
import csv
from pprint import pprint
import mysql.connector

#import sys

filenamepatlst = []
depnamelst = []

i = 0
filename =''
fullfnanbe_sufix = 'budgeted_expenses.csv'
fullfnanf_sufix  = 'funding.csv'
fullfnanoe_sufix = 'organization_budget.csv'
fullfnanof_sufix = 'organization_FTE.csv'

datapath = 'D:\\Storage\\Project and Reasch\\Work Consulting\\Code Sacramento\\sac city budget db visualization\\Data\\'
datafile = datapath +'dir3.txt'

dep_info = {"Mayor Council":                {"org": "/","class": "Admin"},
            "City Attorney":                {"org": "/","class": "Admin"},            
            "City Clerk":                   {"org": "/","class": "Admin"}, 
            "City Manager":                 {"org": "/","class": "Admin"},
            "City Treasurer":               {"org": "/","class": "Admin"},
            
            "Community Development":        {"org": "Manager","class": "Operations"},
            "Convention Culture & Leisure": {"org": "Manager","class": "Operations"},
            "Economic Development":         {"org": "Manager","class": "Operations"},            
            "Fire":                         {"org": "Manager","class": "Operations"},            
            "Parks and Recreation":         {"org": "Manager","class": "Operations"},
            "Police":                       {"org": "Manager","class": "Operations"},
            "Public Works":                 {"org": "Manager","class": "Operations"},
            "Utilities":                    {"org": "Manager","class": "Operations"},
            
            "Finance":                      {"org": "Manager","class": "Support"},
            "General Services":             {"org": "Manager","class": "Support"},
            "Human Resources":              {"org": "Manager","class": "Support"},
            "Information Technology":       {"org": "Manager","class": "Support"} }


def etl_de (csvfile,depname):
    cnx = mysql.connector.connect(user='root', password='root', host='127.0.0.1', database='sac_city_budget')
    query = (
    "LOAD DATA \n"
       "INFILE '" + csvfile.replace('\\', '\\'+'\\') + "'\n"
       "INTO TABLE temp_budget  \n"
         "FIELDS TERMINATED BY ',' "
         "IGNORE 1 LINES\n"
          "(@col1, @col2, @col3, @col4, @col5, @col6, @col7)\n"
          "set    dep      = '"+ depname +"',"
            "org_path             ='"+ dep_info[depname]["org"] +"',"
            "org_clas             = '"+ dep_info[depname]["class"] +"',"
            "budgeted_expenses    = @col1,\n"
            "previousy_actual     = @col2,\n"
            "lasty_approved       = @col3,\n"
            "lasty_amended        = @col4,\n"
            "thisy_approved       = @col5,\n"
            "thisy_amended        = @col6,\n"
            "change_lastthis_amd  = @col7\n" )
    #print query
    cursor = cnx.cursor()
    cursor.execute(query)
    cnx.commit()
    cursor.close()
    cnx.close()

def etl_f (csvfile,depname):
    cnx = mysql.connector.connect(user='root', password='root', host='127.0.0.1', database='sac_city_budget')
    query = (
    "LOAD DATA \n"
       "INFILE '" + csvfile.replace('\\', '\\'+'\\') + "'\n"
       "INTO TABLE temp_budget  \n"
         "FIELDS TERMINATED BY ',' "
         "IGNORE 1 LINES\n"
          "(@col1, @col2, @col3, @col4, @col5, @col6, @col7)\n"
          "set    dep      = '"+ depname +"',"
            "org_path             ='"+ dep_info[depname]["org"] +"',"
            "org_clas             = '"+ dep_info[depname]["class"] +"',"
            "funding              = @col1,\n"
            "previousy_actual     = @col2,\n"
            "lasty_approved       = @col3,\n"
            "lasty_amended        = @col4,\n"
            "thisy_approved       = @col5,\n"
            "thisy_amended        = @col6,\n"
            "change_lastthis_amd  = @col7\n" )
    #print query
    cursor = cnx.cursor()
    cursor.execute(query)
    cnx.commit()
    cursor.close()
    cnx.close()

def etl_oe (csvfile,depname):
    cnx = mysql.connector.connect(user='root', password='root', host='127.0.0.1', database='sac_city_budget')
    query = (
    "LOAD DATA \n"
       "INFILE '" + csvfile.replace('\\', '\\'+'\\') + "'\n"
       "INTO TABLE temp_budget  \n"
         "FIELDS TERMINATED BY ',' "
         "IGNORE 1 LINES\n"
          "(@col1, @col2, @col3, @col4, @col5, @col6, @col7)\n"
          "set    dep      = '"+ depname +"',"
            "org_path             ='"+ dep_info[depname]["org"] +"',"
            "org_clas             = '"+ dep_info[depname]["class"] +"',"
            "organization_budget  = @col1,\n"
            "previousy_actual     = @col2,\n"
            "lasty_approved       = @col3,\n"
            "lasty_amended        = @col4,\n"
            "thisy_approved       = @col5,\n"
            "thisy_amended        = @col6,\n"
            "change_lastthis_amd  = @col7\n" )
    #print query
    cursor = cnx.cursor()
    cursor.execute(query)
    cnx.commit()
    cursor.close()
    cnx.close()
    
def etl_of (csvfile,depname):
    cnx = mysql.connector.connect(user='root', password='root', host='127.0.0.1', database='sac_city_budget')
    query = (
    "LOAD DATA \n"
       "INFILE '" + csvfile.replace('\\', '\\'+'\\') + "'\n"
       "INTO TABLE temp_budget  \n"
         "FIELDS TERMINATED BY ',' "
         "IGNORE 1 LINES\n"
          "(@col1, @col2, @col3, @col4, @col5, @col6, @col7)\n"
          "set    dep      = '"+ depname +"',"
            "org_path             ='"+ dep_info[depname]["org"] +"',"
            "org_clas             = '"+ dep_info[depname]["class"] +"',"
            "organizational_FTE   = @col1,\n"
            "previousy_actual     = @col2,\n"
            "lasty_approved       = @col3,\n"
            "lasty_amended        = @col4,\n"
            "thisy_approved       = @col5,\n"
            "thisy_amended        = @col6,\n"
            "change_lastthis_amd  = @col7\n" )
    #print query
    cursor = cnx.cursor()
    cursor.execute(query)
    cnx.commit()
    cursor.close()
    cnx.close()
    
# main
with open(datafile, 'rb') as csvfile:
    xyreader = csv.reader(csvfile, delimiter=',', quotechar='"')
    for row in xyreader:
        if ( row[0].split(fullfnanbe_sufix, ).__len__() == 2):
            # exstract depname from dir output by splitting and stripping each dir line
            depname = row[0].split('CSFB-2012-13-', )[1].split('-', )[1].split(fullfnanbe_sufix, )[0].strip()            
            #print depname,":",dep_info[depname]["org"],',', dep_info[depname]["class"];
            fnam = 'CSFB' + row[0].split('CSFB', )[1].split(fullfnanbe_sufix, )[0]
            filenamepatlst.append(datapath + fnam) 
            depnamelst.append( depname)
            
for i in range(0,filenamepatlst.__len__()):                    
    # for each base depname there are 4 types and 4 .csv file names
    print "ETL lode for:", depnamelst[i]
    #print depnamelst[i]
    fullfnanbe = filenamepatlst[i] + fullfnanbe_sufix
    fullfnanf  = filenamepatlst[i] + fullfnanf_sufix
    fullfnanoe = filenamepatlst[i] + fullfnanoe_sufix
    fullfnanof = filenamepatlst[i] + fullfnanof_sufix
    etl_de (fullfnanbe,depnamelst[i])
    etl_f (fullfnanf,depnamelst[i])
    etl_oe (fullfnanoe,depnamelst[i])
    etl_of (fullfnanof,depnamelst[i])
  
print filenamepatlst.__len__() , '\n'




    