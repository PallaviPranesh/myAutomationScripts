'''
Created on April 28, 2020
@author: ppranish
'''
import json
from collections import OrderedDict

from pyzabbix import ZabbixAPI
from concurrent.futures import ThreadPoolExecutor, wait, as_completed
from ConfigFileParser import ConfigFileParser
from datetime import datetime
from pandas.core.frame import DataFrame
import logging
import urllib3,os
import repackage
repackage.up()
from Zabbix_NI_API_test.getvszZoneCount_36 import *
import numpy as np
urllib3.disable_warnings()
configParser = ConfigFileParser()

global apDetailsDataFrame
global zabbixDataFrame,pytest

# function which given a host and zabbix connection gets the macros in itself, its parents and their parents. 
def getMacros(zh,host):
    hostDetails = zh.host.get(hostids=[host['hostid']],selectMacros=["macro","value"],selectParentTemplates=["name"])
    hostMacros = hostDetails[0]['macros']
    return hostMacros

def replaceCharInStr(inputStr,inputChar):
    oldStr = inputStr
    inputStr = inputStr.replace(inputChar, "").strip();
    if inputStr.isdigit():
        oldStr = inputStr
        print("Replacing "+inputChar+" in the string: "+oldStr)
    else:
        print("Not Replacing "+inputChar+" in the string: "+oldStr+" as it is not numeric after replacing char.")
    return oldStr

def hostAPThread(zh,host,refreshTime):
    hostName = host['host']
    status = False
    try:
        hostMacros = getMacros(zh, host)
        
        zoneName = 'NA'
        custAccNumber = 'NA'
        parentID = 'NA'
        circuitID = 'NA'
        zoneLongitude = -84.15625
        zoneLatitude = 34.06460
        zoneID = 'NA'
        for hostMacro in hostMacros:
            if hostMacro['macro'] == "{$ZONE_NAME}":
                zoneName = hostMacro['value']
            elif hostMacro['macro'] == "{$ZONE_LONGITUDE}":
                try:
                    zoneLongitude = float(hostMacro['value'])
                except Exception as exc:
                    logging.info("Zone longitude might be empty")
                zoneLongitude = round(zoneLongitude, 6)
            elif hostMacro['macro'] == "{$ZONE_LATITUDE}":
                try:
                    zoneLatitude = float(hostMacro['value'])
                except Exception as exc:
                    logging.info("Zone latitude might be empty")
                zoneLatitude = round(zoneLatitude, 6)
            elif hostMacro['macro'] == "{$ICOMS_NUMBER}":
                custAccNumber = hostMacro['value']
            elif hostMacro['macro'] == "{$PARENT_ID}":
                parentID = hostMacro['value']
            elif hostMacro['macro'] == "{$CIRCUIT_ID}":
                circuitID = hostMacro['value']
        
        if zoneName == 'NA' or zoneName == '':
            logging.error("$ZONE_NAME field not configured for host %s. It will be ignored.", hostName)
        else:
            logging.info("$ZONE_NAME field is configured for host %s with %s.", hostName, zoneName)
        
        if zoneLongitude == -84.15625:
            logging.error("$ZONE_LONGITUDE field not configured for host %s. It will be ignored.", hostName)
        else:
            logging.info("$ZONE_LONGITUDE field is configured for host %s with %s.", hostName, zoneLongitude)

        if zoneLatitude == 34.06460:
            logging.error("$ZONE_LATITUDE field not configured for host %s. It will be ignored.", hostName)
        else:
            logging.info("$ZONE_LATITUDE field is configured for host %s with %s.", hostName, zoneLatitude)

        if parentID == 'NA' or parentID == '':
            logging.error("$PARENT_ID field not configured for host %s. It will be ignored.", hostName)
        else:
            logging.info("$PARENT_ID field is configured for host %s with %s.", hostName, parentID)
        
        if circuitID is None:
            logging.error("$CIRCUIT_ID field not configured for host %s. It will be ignored.", hostName)
        else:
            logging.info("$CIRCUIT_ID field is configured for host %s with %s.", hostName, circuitID)

        if custAccNumber == 'NA' or custAccNumber == '':
            logging.info('$ICOMS_NUMBER field not configured for host %s. It will be ignored. ', hostName)
            return status
        
        custAccNumber = replaceCharInStr(custAccNumber,"-")
        
        apOpsDF = DataFrame()
        apmacList = []
        try:

            apOpsDF = apOpsDF.append({'realm': custAccNumber, 'ZabbixZoneName':zoneName}, ignore_index=True)
                
            # else:
            #     #No Access points for the host. So, we are hard coding the value as '0'
            #     apOpsDF = apOpsDF.append({'realm': custAccNumber, 'ZabbixZoneName':zoneName}, ignore_index=True)
                
            status = True
            
        except Exception as exc:
            logging.exception("Exception while retrieving data for Access Points %s",hostName)
            #No Access points for the host. So, we are hard coding the value as '0'
            status = True
            apOpsDF = apOpsDF.append({'realm': custAccNumber, 'ZabbixZoneName':zoneName}, ignore_index=True)
            return status,apOpsDF
    except Exception as exc:
        logging.exception("Exception while retrieving data for host %s",hostName)
    
    return status,apOpsDF


# function called for each template found under the property host group
def propTemplateFunc(zh,template,propHostGroup,maxZabbixWorker):
    status=False
    selectedHostList = []
    try:
        templateName = template['name']
        matchingTemplates = zh.template.get(filter= {"name": [templateName]},selectMacros=["macro","value"])
        if len(matchingTemplates) != 0:
            template = matchingTemplates[0]
            matchingTemplateMacros = template['macros']
            hostGroupName = None
            zoneName = None
            zoneLongitude = -84.15625
            zoneLatitude = 34.06460
            custAccNo = 'NA'
            # from the template get the macros and check for a match 
            
            for macro in matchingTemplateMacros:
                if macro['macro'] == "{$ZONE_NAME}":
                    zoneName = macro['value']
                elif macro['macro'] == "{$ZONE_LONGITUDE}":
                    try:
                        zoneLongitude = float(macro['value'])
                    except Exception as exc:
                        logging.info("Zone longitude might be empty")
                    zoneLongitude = round(zoneLongitude, 6)
                elif macro['macro'] == "{$ZONE_LATITUDE}":
                    try:
                        zoneLatitude = float(macro['value'])
                    except Exception as exc:
                        logging.info("Zone longitude might be empty")
                    zoneLatitude = round(zoneLatitude, 6)
                elif macro['macro'] == "{$PROPERTY_HOST_GROUP}":
                    hostGroupName = macro['value']
                elif macro['macro'] == "{$ICOMS_NUMBER}":
                    custAccNo = macro['value']
            hostListDF = []

            if custAccNo != 'NA' and custAccNo!='' and  zoneName is not None or zoneName != 'NA':
                custAccNo = replaceCharInStr(custAccNo,"-")
           
            if zoneName is None or zoneName == 'NA' or hostGroupName is None or custAccNo == 'NA' or custAccNo=='':
                logging.info("Skipping template %s since zone name or host group or $ICOMS_NUMBER macro is missing",templateName)
            else:
                executor = ThreadPoolExecutor(max_workers=maxZabbixWorker)
                obj = zh.hostgroup.get(filter= {"name": [hostGroupName]},selectHosts=["name","host"])
                hostListDF = DataFrame()
                for propIndex in range(0,len(obj)):
                    # get all hosts under the host group
                    hostList = obj[propIndex]['hosts']
                    logging.info("Host list before filter: " + str(hostList))

#                     hostGroupName = obj[propIndex]['name']
#                     logging.info("Processing device info & stats for host group: %s",hostGroupName)
                    #here looping the host list to verify the template what we found is matching with vsz Templates or not
                    #if it is matching then take the host to get the Access point details

                    for i in range(len(hostList)):
                        host = hostList[i]
                        hostTemplates = zh.template.get(hostids=host['hostid'])
                        hostTemplateNames = [template['name'] for template in hostTemplates]
                        print("hostTemplateNames: " + str(hostTemplateNames))
                        templateNames = configParser.getZabbixVSZTemplates()
                        validTemplates = [template for template in hostTemplateNames if template in templateNames]
                        logging.info("validTemplates: " + str(validTemplates))
                        if isinstance(validTemplates, list) and len(validTemplates) != 0:
                            selectedHostList.append(host)
                            logging.info("selectedHostList: " + str(selectedHostList))
                        else:
                            logging.info("else part of validTemplates: " + str(validTemplates))
                executor.shutdown()
            status = True
        else:
            logging.error("Template %s not found ",templateName)
    except Exception as exc:
        logging.exception("propTemplateFunc: Exception while getting ")
        
    return status,selectedHostList
#
def dump_ap_stats_by_host(hostLists):
    print("dump_ap_stats_by_host: ap details dump started")
    zoneDataFrame = DataFrame()
    maxZabbixWorker = 5
    now = datetime.now()
    currentTime = now
    print("dump_ap_stats_by_host:Connecting to Zabbix..")
    zh = ZabbixAPI(configParser.getZabbixURL())
    zh.login(configParser.getZabbixUsername(), configParser.getZabbixPassword())
    refreshTime = datetime.utcnow()
    refreshTime = refreshTime.replace(tzinfo=None)

    executor = ThreadPoolExecutor(max_workers=maxZabbixWorker)
    try:
        hostFutures = {executor.submit(hostAPThread, zh,host,refreshTime): host for host in hostLists}

        for future in as_completed(hostFutures):

            hostVal = hostFutures[future]
            try:
                status, apDetailsDataFrame = future.result()
                if status is True:
                    if not apDetailsDataFrame.empty:
                        zoneDataFrame = zoneDataFrame.append(apDetailsDataFrame, ignore_index=True)
                    else:
                        print("Data Frame is Empty")
                else:
                    print("Host: %s failed in recovering data",hostVal)
            except Exception as exc:
                print("Host: %s generated an exception",hostVal)
        executor.shutdown()
        zoneDataFrame = zoneDataFrame.reindex(columns=['realm', 'ZabbixZoneName'])
        print("length: "+str(len(zoneDataFrame)))
        print("zoneDataFrame details: "+zoneDataFrame.to_string())
        # zoneDataFrame.to_csv("propertyAPCount.csv",index=False)
    except Exception as exc:
        print("Exception while fetching AP details from Zabbix." + str(exc))
    return zoneDataFrame

def dump_property_stats():
    print("Calling stats dump")
    maxZabbixWorker = 5
    zoneDataFrame2 = DataFrame()
    print("dump_property_stats:Connecting to Zabbix..")
    zh = ZabbixAPI(configParser.getZabbixURL())
    zh.login(configParser.getZabbixUsername(),configParser.getZabbixPassword())

    print("dump_property_stats: Zabbix connected. Fetching host groups..")

    preparedHostList = []

    try:
        properties = configParser.getZabbixPropTemplates()
        executor = ThreadPoolExecutor(max_workers=maxZabbixWorker)
        # get the list of zProperty templates in the host group
        obj = zh.hostgroup.get(filter={"name": properties}, selectTemplates=["name"])

        for propIndex in range(0, len(obj)):
            print("Processing templates for host group: %s", obj[propIndex]['name'])
            templateList = obj[propIndex]['templates']
            # check for maintenance host groups
            maintenanceProperties = [obj[propIndex]['name'] + '-Maint']
            maintenancePropertiesObj = zh.hostgroup.get(filter={"name": maintenanceProperties}, selectTemplates=["name"])
            maintenanceTemplateList = []
            for maintenancePropertiespropIndex in range(0, len(maintenancePropertiesObj)):
                    maintenanceTemplateList += maintenancePropertiesObj[maintenancePropertiespropIndex]['templates']
                # need to exclude those templates in the maintenance host group
            print("templateList before " + str(templateList))
            print("maintenanceTemplateList before " + str(maintenanceTemplateList))
            newTemplateList = []
            for template in templateList:
                    found = False
                    print("template before " + str(template))
                    for maintTemplate in maintenanceTemplateList:
                        print("maintTemplate before " + str(maintTemplate))
                        if template['name'] == maintTemplate['name'] and template['templateid'] == maintTemplate[
                            'templateid']:
                            found = True
                            break
                    if found is False:
                        newTemplateList.append(template)

            templateList = newTemplateList
            templateListDF = DataFrame()
            # logging.error("templateList after " + str(templateList))
            print("templateList after len " + str(len(templateList)))
            # loop through main template list to get the stats and other information
            hostFutures = {executor.submit(propTemplateFunc, zh, template, obj[propIndex]['name'], maxZabbixWorker): template for
            template in templateList}
            for future in as_completed(hostFutures):
                    template = hostFutures[future]

                    try:
                        status, listOfHosts = future.result()


                        if status is True:
                            if isinstance(listOfHosts, list) and len(listOfHosts) != 0:
                                preparedHostList.extend(listOfHosts)
                            else:
                                print("Host list is Empty")
                        else:
                            print("Host: %s failed in recovering data", template['name'])

                    except Exception as exc:
                        print('%s generated an exception ', template['name'])

        # print(hostListDF.to_string())
        executor.shutdown()
        zabbixDataFrame = dump_ap_stats_by_host(preparedHostList)
    except Exception as exc:
        print("dump_property_stats: Exception is ")
    return zabbixDataFrame

print("Start time of tool: " + str(datetime.now()))
print("Calling ap details dump")


ZabbixDataFrame= dump_property_stats()


for key, value in configParser.getVSZDetails().items():
        input_dict = OrderedDict([(key, value)])
        output_dict = json.loads(json.dumps(input_dict))
VszZoneNameList = []
VszZoneNameListDF = DataFrame()
keyDetails = output_dict['VszHost']
for eachSubKeys in keyDetails:
    for key in eachSubKeys.keys():
            getZoneVSZAPI = getZoneFromVSZAPI_36(eachSubKeys[key])
            VszZoneNameList= getZoneVSZAPI.get_vszZones(eachSubKeys[key])
            print("VszHOstLen of " +  ":==" + str(len(VszZoneNameList)))

            for i in range(0,len(VszZoneNameList)):
                VszZoneNameListDF = VszZoneNameListDF.append({'VSZZoneName': VszZoneNameList[i]}, ignore_index=True)
#

# ZabbixDataFrame.sort_values("ZabbixZoneName", inplace=True)
# ZabbixDataFrame.sort_values("VSZZoneName", inplace=True)
print(VszZoneNameListDF.to_string())
ZabbixDataFrame.to_csv(os.path.join(os.getcwd(), os.pardir, 'csvOutput', 'Zabbix-VSZCluster-Zones.csv'),index=False)
#

NIStatus = []
ZabbixDataFrame = ZabbixDataFrame.merge(VszZoneNameListDF, left_on='ZabbixZoneName', right_on='VSZZoneName', how='outer')
ZabbixDataFrame = ZabbixDataFrame.replace(np.NAN, 'Not Present')
ZabbixDataFrame.fillna(0)
for index, row in ZabbixDataFrame.iterrows():
    if  row['ZabbixZoneName'] == row['VSZZoneName']:
        NIStatus.append('Match')
    else:
        NIStatus.append('No Match')
ZabbixDataFrame['Status'] = NIStatus
print(ZabbixDataFrame.to_string())
ZabbixDataFrame.to_csv(os.path.join(os.getcwd(), os.pardir, 'csvOutput', 'Zabbix-vs-VSZ-Zones.csv'),index=False)