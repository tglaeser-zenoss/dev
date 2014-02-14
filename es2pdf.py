#!/usr/bin/python

#################################
# elasticsearch to PDF exporter #
#    (c) Taylor Glaeser         #
#        Zenoss, Inc.           #
#        2014                   #
#################################


import json
import requests # Non-standard library
import ConfigParser
import argparse
from fpdf import FPDF  # Non-standard library
from time import time, gmtime, strftime

TITLE_PDF = ""  #Title of the PDF
ES_QUERY = ""  #Elasticsearch Query
TITLE_MSG = ""  #Title of "message" field  ### NEED TO BE REMOVED
TITLE_STACK = ""  #Title of "stack field   ### NEED TO BE REMOVED
PDF_SAVE_TEMPLATE = ""  #How the PDF file name will appear when saved
LETTERHEAD_PNG = ""  #PNG file for the letter head in PDF
ES_IP_ADDR = ""  #IP Address and port of elasticsearch
MAX_RESULT = 0  #How many results are actually printed
SEARCH_DAY = ""  #Date of index suffix to search from
FIELDS = {}  #Fields from elasticsearch to search for and print to the PDF
FIELDS_ORDERED = []  #Fields from elasticsearch ordered in the order from config
FIELDS_NUMBERED = {}  #Fields from elasticsearch but with an index number (i.e. value0, value1, value2, etc)
LIST_FIELDS = False  #Whether or not to list all available fields to print from elasticsearch query


globalList = {}  #List used to store parsed JSON  (values are in format "value0", "value1", "value2", etc...)
globalListFields = {}  #List used to store parsed fields (values are only stored as "value")


#jsontext = {"took":24,"timed_out":False,"_shards":{"total":5,"successful":5,"failed":0},"hits":{"total":2095,"max_score":1.426525,"hits":[{"_index":"logstash-2014.02.11","_type":"syslog","_id":"guWfQ34zR66EkokzM0Hmrg","_score":1.426525, "_source" : {"@source":"tcp://10.55.134.144:54849/","@tags":[],"@fields":{"stack":"UMUC3","node":"UMUC3\n","pri":["133"], "subdict2" : {"subkey1" : "taylortest1", "subkey2" : "taylortest2", "subkey3":"taylortest3"     },"logsource":["localhost"],"program":["opt-zenoss-log-zenjobs"],"zenseverity":["INFO"],"zendaemon":["relstorage"],"message":["Reconnected. stack=UMUC3 node=UMUC3"]},"@timestamp":"2014-02-11T14:00:28.000Z","@source_host":"10.55.134.144","@source_path":"/","@message":"<133>2014-02-11T14:00:28.612170+00:00 localhost opt-zenoss-log-zenjobs 2014-02-11 14:00:19,406 INFO relstorage: Reconnected. stack=UMUC3 node=UMUC3\n","@type":"syslog"}}]}}
#  ABOVE TEXT IS FOR ONE RESULT

jsontext = {"took":20,"timed_out":False,"_shards":{"total":5,"successful":5,"failed":0},"hits":{"total":2095,"max_score":1.426525,"hits":[{"_index":"logstash-2014.02.11","_type":"syslog","_id":"guWfQ34zR66EkokzM0Hmrg","_score":1.426525, "_source" : {"@source":"tcp://10.55.134.144:54849/","@tags":[],"@fields":{"stack":"UMUC3","node":"UMUC3\n","pri":["133"],"logsource":["localhost"],"program":["opt-zenoss-log-zenjobs"],"zenseverity":["INFO"],"zendaemon":["relstorage"],"message":["Reconnected. stack=UMUC3 node=UMUC3"]},"@timestamp":"2014-02-11T14:00:28.000Z","@source_host":"10.55.134.144","@source_path":"/","@message":"<133>2014-02-11T14:00:28.612170+00:00 localhost opt-zenoss-log-zenjobs 2014-02-11 14:00:19,406 INFO relstorage: Reconnected. stack=UMUC3 node=UMUC3\n","@type":"syslog"}},{"_index":"logstash-2014.02.11","_type":"syslog","_id":"B2LkhqpQRKuRK55IVu7Jog","_score":1.4203708, "_source" : {"@source":"tcp://10.55.134.144:54849/","@tags":[],"@fields":{"stack":"UMUC3","node":"UMUC3\n","pri":["133"],"logsource":["localhost"],"program":["opt-zenoss-log-zenjobs"],"zenseverity":["INFO"],"zendaemon":["relstorage"],"message":["Reconnected. stack=UMUC3 node=UMUC3"]},"@timestamp":"2014-02-11T14:00:28.000Z","@source_host":"10.55.134.144","@source_path":"/","@message":"<133>2014-02-11T14:00:28.612158+00:00 localhost opt-zenoss-log-zenjobs 2014-02-11 14:00:19,239 INFO relstorage: Reconnected. stack=UMUC3 node=UMUC3\n","@type":"syslog"}},{"_index":"logstash-2014.02.11","_type":"syslog","_id":"FZDruiXGTYaLRargBrssrQ","_score":1.4203708, "_source" : {"@source":"tcp://10.55.134.144:54849/","@tags":[],"@fields":{"stack":"UMUC3","node":"UMUC3\n","pri":["133"],"logsource":["localhost"],"program":["opt-zenoss-log-zenjobs"],"zenseverity":["INFO"],"zendaemon":["relstorage"],"message":["Reconnected. stack=UMUC3 node=UMUC3"]},"@timestamp":"2014-02-11T14:00:28.000Z","@source_host":"10.55.134.144","@source_path":"/","@message":"<133>2014-02-11T14:00:28.612196+00:00 localhost opt-zenoss-log-zenjobs 2014-02-11 14:00:23,595 INFO relstorage: Reconnected. stack=UMUC3 node=UMUC3\n","@type":"syslog"}},{"_index":"logstash-2014.02.11","_type":"syslog","_id":"gEiQn0RGRBe07K-91eKuyQ","_score":1.4203708, "_source" : {"@source":"tcp://10.55.134.144:54849/","@tags":[],"@fields":{"stack":"UMUC3","node":"UMUC3\n","pri":["133"],"logsource":["localhost"],"program":["opt-zenoss-log-zenjobs"],"zenseverity":["INFO"],"zendaemon":["relstorage"],"message":["Reconnected. stack=UMUC3 node=UMUC3"]},"@timestamp":"2014-02-11T14:00:28.000Z","@source_host":"10.55.134.144","@source_path":"/","@message":"<133>2014-02-11T14:00:28.612216+00:00 localhost opt-zenoss-log-zenjobs 2014-02-11 14:00:23,710 INFO relstorage: Reconnected. stack=UMUC3 node=UMUC3\n","@type":"syslog"}},{"_index":"logstash-2014.02.11","_type":"syslog","_id":"-GpUZe9LRgSWwR0mvEgEpw","_score":1.4203708, "_source" : {"@source":"tcp://10.55.134.144:54849/","@tags":[],"@fields":{"stack":"UMUC3","node":"UMUC3\n","pri":["133"],"logsource":["localhost"],"program":["opt-zenoss-log-zenjobs"],"zenseverity":["INFO"],"zendaemon":["relstorage"],"message":["Reconnected. stack=UMUC3 node=UMUC3"]},"@timestamp":"2014-02-11T14:00:18.000Z","@source_host":"10.55.134.144","@source_path":"/","@message":"<133>2014-02-11T14:00:18.604107+00:00 localhost opt-zenoss-log-zenjobs 2014-02-11 14:00:18,089 INFO relstorage: Reconnected. stack=UMUC3 node=UMUC3\n","@type":"syslog"}}]}}
#  ABOVE TEST IS FOR FIVE RESULTS


## FIELDS FORMAT
#############################################
#                                           #
#{"json_item" : ["PDF Title", "json_text"]} #
#                                           #
#############################################





DEFAULT_CONFIG = "es2pdf.cfg"  #Default config file
DEFAULT_SECTION = "Defaults"  #Default section of config file
DEFAULT_INDEX = "logstash-"  #Default index prefix

#  IMPORTANT TAGS FOR PRODUCTION ELASTIC SEARCH
#
#  _source -> @fields
#  @fields -> stack, message














def sort_keys(keyDict, curResult):
    d_sub = keyDict["hits"]["hits"][curResult]["_source"]
    global globalListy
    global FIELDS_NUMBERED

    globalList = {}

    has_fields_stored = False

    def recurse(curDict, depth=-1, currentDictKey="", r=0):
        if depth != 0:
            #print "Recurse is " + str(r)
            #print "currentDictKey: " + str(currentDictKey)
            if r > 0:

                #print "currentDictKey key: " + str(curDict.get(currentDictKey))
                #print "currentDictKey key type: " + str(type(curDict.get(currentDictKey))) + "END TYPE"
            
                #raw_input()  #Used to view each entry
                if (type(curDict.get(currentDictKey)) is dict) and (len(curDict.get(currentDictKey)) > 0):
                    #print "IS DICT!!\n\n"
                    #print str(curDict.get(currentDictKey))

                    curDict_sub = curDict.get(currentDictKey) #Set sub dict as primary dict

                    for i in dict(curDict_sub):
                        
                        #print "i: " + str(i)
                        recurse(curDict_sub, depth-1, curDict_sub, 0)
                else:

                    if (type(curDict.get(currentDictKey)) is list):
                        list_dict = curDict.get(currentDictKey)
                        if len(list_dict) > 0:
                            #print "list_dict : " 
                            #print list_dict[0]
                            list_dict_str = str(list_dict[0])
                            #print "list_dict_str : "
                            #print list_dict_str
                        else:
                            list_dict_str = ""
                        globalList[currentDictKey+str(curResult)] = list_dict_str.replace("\n", "")
                        globalListFields[currentDictKey] = list_dict_str.replace("\n", "")
                    elif (type(curDict.get(currentDictKey)) is str):
                        globalList[currentDictKey+str(curResult)] = str(curDict.get(currentDictKey)).replace("\n", "")
                        globalListFields[currentDictKey] = str(curDict.get(currentDictKey)).replace("\n", "")
                    else: 
                        globalList[currentDictKey+str(curResult)] = str(curDict.get(currentDictKey))
                        globalListFields[currentDictKey] = str(curDict.get(currentDictKey))
                    #print "globalList: " + str(globalList)

            else:
                for i in currentDictKey:
                        #print "i(r=0): " + str(i)
                        recurse(curDict, depth-1, i, r+1)

    recurse(d_sub, -1, d_sub)

    #print globalList

    ##### CHANGE TO ADDING ITEMS TO REQUESTED ITEMS
       
    if LIST_FIELDS is True:
            print "##################################\n###   BEGIN AVAILABLE FIELDS   ###"         
            print "##################################\n### Template                   ###\n### Field : Current field data ###\n##################################"
            for i in globalListFields:   
                print "### " + str(i) + " : " + str(globalListFields[i])
            print "##################################\n###    END AVAILABLE FIELDS    ###\n##################################"
            exit()

    
    for i in FIELDS_ORDERED:   # Selective printing
        try:
            #print str(i) + " : " + str(globalList[i])
            FIELDS_NUMBERED[i[0]+str(curResult)] = [FIELDS[i[0]][0],str(globalList.get(i[0]+str(curResult)))]

            #FIELDS[i+str(curResult)].append(str(globalList.get(i+str(curResult))))
            
            if FIELDS_NUMBERED[i[0]+str(curResult)][1] == "None":
                FIELDS_NUMBERED[i[0]+str(curResult)][1] = "Not a valid field!!"
            #print i[0]+str(curResult) + " : " + str(FIELDS_NUMBERED[i[0]+str(curResult)])
        except KeyError, e:
            raise e
            #print "No such value " + str(i)
        has_fields_stored = True


    return

















def parseJSON(d):  # d is the dictionary of values that was returned from the HTTP GET request
    if MAX_RESULT == "ALL":
        totalResults = len(d["hits"]["hits"])
    else:
        totalResults = MAX_RESULT
    
    if totalResults <= 0:
        print "No results."
        exit()

    else:
        for i in range(totalResults):
            sort_keys(d, i)
        cellInfo = {}
        print FIELDS_NUMBERED
        print totalResults

        for i in range(totalResults):     # GENERATE MESSAGE
            cellInfo["res"+str(i)] = ""
            for j in FIELDS_ORDERED:
                cellInfo["res"+str(i)] += FIELDS_NUMBERED[j[0]+str(i)][0] + FIELDS_NUMBERED[j[0]+str(i)][1] + "\n"
            print "Result " + str(i+1) + ": \n" + cellInfo["res"+str(i)]
        
        
        createPDF(cellInfo)

def createPDF(values):
    pdf = FPDF()
    pdf.add_page()
    pdf.set_font("Arial", "B", 24)
    pdf.cell(0, 20, TITLE_PDF, border=0,align="C", ln=1)
    #pdf.image(LETTERHEAD_PNG,5, 5, 0, 0)     #### COMENTED OUT TO DUE NO IMAGE
    pdf.set_font("Courier", "", 10)
    i = 0
    for i in range(len(values)):
        pdf.multi_cell(0, 6, values["res"+str(i)], border=1)  ## Must be dynamic
        i += 1

    curUTC = strftime("%Y-%m-%dT%H-%M-%SZ", gmtime(time()))
    pdf.output(PDF_SAVE_TEMPLATE + "-" + curUTC + ".pdf", "F")


def startQuery(q=ES_QUERY, r=MAX_RESULT, ip=ES_IP_ADDR, index=DEFAULT_INDEX, date=None, fields=FIELDS):
 
    if q == None: q = ES_QUERY
    if r == None: r = MAX_RESULT
    if ip == None: ip = ES_IP_ADDR
    
    if index != None: 
        if index != DEFAULT_INDEX:
            date = "OMIT"
    elif index == None:
        index = DEFAULT_INDEX

    if date == None:
        date = strftime("%Y.%m.%d", gmtime(time() - 86400))
    elif date == "OMIT":
        date = ""

    print "http://" + ip + "/" + index + date + "/_search?size=" + str(r) + "&q=" + q
    
    r = requests.get("http://" + ip + "/" + DEFAULT_INDEX + date + "/_search?size=" + str(r) + "&q=" + q)   # Need to change this to dynamic date

    parseJSON(r.json())

    #parseJSON(jsontext)  # UNCOMMENT TO TEST


def parseConfig(c=DEFAULT_CONFIG, s=DEFAULT_SECTION):
    try:
        config = ConfigParser.RawConfigParser()
        config.read(c)
        
        if s == None : s = DEFAULT_SECTION

        global TITLE_PDF
        global ES_QUERY
        global TITLE_MSG
        global TITLE_STACK
        global PDF_SAVE_TEMPLATE
        global LETTERHEAD_PNG
        global ES_IP_ADDR
        global MAX_RESULT
        global SEARCH_DAY
        global FIELDS  # Need to add this to config file
        global FIELDS_ORDERED

        TITLE_PDF = config.get(s, "title_of_PDF")
        ES_QUERY = config.get(s, "elasticsearch_query")
        TITLE_MSG = config.get(s, "title_of_message_field")
        TITLE_STACK = config.get(s, "title_of_stack_field")
        PDF_SAVE_TEMPLATE = config.get(s, "pdf_filename_prefix")
        LETTERHEAD_PNG = config.get(s, "letterhead_image")
        ES_IP_ADDR = config.get(s, "elasticsearch_ip")
        
        if config.get(s, "max_search_results") == "all":
            MAX_RESULT = "ALL"
        else:
            MAX_RESULT = config.getint(s, "max_search_results")
        

        SEARCH_DAY = config.get(s, "index_day_to_search")
        
        fields_list = config.items("fields_to_print")
        FIELDS_ORDERED = fields_list

        #config.items("dev_fields_to_print")
        #stdout => [('@fields.stack', 'Stack:'), ('@message', 'Message:'), ('@timestamp', 'Timestamp:')]

        for i in fields_list:
            FIELDS[i[0]] = [i[1]]
        
        #print FIELDS

        if SEARCH_DAY == "yesterday":
            SEARCH_DAY = strftime("%Y.%m.%d", gmtime(time() - 86400))

    except ConfigParser.NoSectionError as e:
        print "No config file found..."
        exit()
        


    # Remove before production #
    #print TITLE_PDF            #
    #print ES_QUERY             #
    #print TITLE_MSG            #
    #print TITLE_STACK          #
    #print PDF_SAVE_TEMPLATE    #
    #print LETTERHEAD_PNG       #
    #print ES_IP_ADDR           #
    #print MAX_RESULT           #
    #print SEARCH_DAY           #
    # Remove before production #


def parseArgs():
    
    global LIST_FIELDS

    parser = argparse.ArgumentParser(description="Output an elasticsearch query to PDF")
    
    parser.add_argument("-c", "--config", dest="config", default=DEFAULT_CONFIG, metavar="CONFIG", help="Change what config file to use")
    parser.add_argument("-s", "--section", dest="section", default=None, metavar="SECTION", help="Change what config section to use")
    parser.add_argument("-ip", "--ipaddress", dest="ip", default=None, metavar="IPADDR:PORT", help="Change what IP address and port is used to connect to elasticsearch")
    parser.add_argument("-i", "--index", dest="index", default=DEFAULT_INDEX, metavar="INDEX", help="Change what index to query (Default is \'logstash-\')")
    parser.add_argument("-d", "--date", dest="date", default=None, metavar="YYYY.MM.DD", help="Change what index date to query (Default is yesterday)")
    parser.add_argument("-q", "--query", dest="query", default=None, metavar="QUERY", help="Change what query to execute")
    parser.add_argument("-r", "--results", dest="results", default=None, metavar="RESULTS", help="Change the number of results requested")
    parser.add_argument("-f", "--fields", action="store_true", help="List all available fields to output from elasticsearch query")

    args_ = parser.parse_args()


    LIST_FIELDS = vars(args_)["fields"]
    
    if vars(args_)["date"] == "yesterday":
            vars(args_)["date"] = strftime("%Y.%m.%d", gmtime(time() - 86400))

    return vars(args_)


if __name__ == '__main__':
    args = parseArgs()
    parseConfig(args["config"], args["section"])
    startQuery(args["query"], args["results"], args["ip"], args["index"], args["date"])


