'''
This is the preprocessing file intended to be run on a scenario before
running the web application on that scenario. Please refer to the user
guide for more information.
'''

import sys
import time

import mysql.connector
import requests
from mysql.connector import Error

from lib.config import (ANNOTATIONS_URL, API_KEY, CLEANUP_QUERIES, COLORS,
                        DATABASE, DATASOURCE_URL, HOST, INTERVAL,
                        LEFT_THRESHOLD, MAX_LEGAL_UNIX_TIMESTAMP, MAX_WIDTH,
                        RIGHT_THRESHOLD, UNIX_TIME_START_YEAR, URL, YEAR_SEC,
                        headers, MYSQL_USER, MYSQL_PASSWORD)
from lib.core import (Flow, Grafana_Dashboard, Grafana_Dashboard_Properties,
                      Grafana_Datasource, Grafana_Grid_Position, Grafana_Panel,
                      Grafana_Target, Grafana_Time, MySQL_Manager,
                      QueryBuilder, Switch)


def main():

    mysql_manager = MySQL_Manager(database=DATABASE, user=MYSQL_USER, password=MYSQL_PASSWORD)

    for query in CLEANUP_QUERIES:
        mysql_manager.execute_query(query)
    
    map_ip = getIpAddresses(mysql_manager)

    switch_map = initializeSwitches(mysql_manager)
    flow_map = initializeFlows(mysql_manager)

    for switch in switch_map:
        switch_map[switch].populateFlowList(mysql_manager)
        switch_map[switch].populateRatios(mysql_manager)
        switch_map[switch].print_info(map_ip)
        

    for flow in flow_map:
        flow_map[flow].populateSwitchList(mysql_manager)
        flow_map[flow].populateRatios(mysql_manager)
        flow_map[flow].print_info(map_ip) 

    # Get switch from which the trigger originated
    result = mysql_manager.execute_query('select switch from triggers')
    trigger_switch = result[1:][0][0]
    
    # Get peak queue depth
    result_set = mysql_manager.execute_query('select max(queue_depth) from packetrecords')
    peak_queue_depth = result_set[1][0]
    print("Peak Queue Depth: " + str(peak_queue_depth))

    result_set = mysql_manager.execute_query('select time_in, time_out, queue_depth from packetrecords where queue_depth = ' + str(peak_queue_depth))
    time_in_at_peak = result_set[1][0]
    time_out_at_peak = result_set[1][1]
    print("\nTime of peak depth: " + str(time_out_at_peak))

    # Obtain set of queue depths at different times, ordered by time out.
    result_set = mysql_manager.execute_query('select time_in, time_out, queue_depth from packetrecords where switch = \'' + trigger_switch + '\' order by time_out')

    # Find index in result_set
    index_of_peak = result_set.index( (time_in_at_peak, time_out_at_peak, peak_queue_depth) )

    # Initialize left and right indices
    left_pointer = index_of_peak - 1
    right_pointer = index_of_peak + 1

    # Calculate left index
    while left_pointer >= 0 and result_set[left_pointer][2] > LEFT_THRESHOLD * peak_queue_depth:
        left_pointer = left_pointer - 1

    # Calculate right index
    while right_pointer < len(result_set) and result_set[right_pointer][2] > RIGHT_THRESHOLD * peak_queue_depth:
        right_pointer = right_pointer + 1

    # Ensure you haven't gone beyond limits
    right_pointer = (right_pointer - 1) if right_pointer == len(result_set) else right_pointer
    left_pointer = (left_pointer + 1) if left_pointer == 0 else left_pointer

    left_time = result_set[left_pointer][0]
    right_time = result_set[right_pointer][1]

    width_of_peak = right_time - left_time

    print("\nLeft time: {} Right time: {} Time difference: {} microseconds".format(str(left_time), str(right_time), str(width_of_peak/1000)))

    result_file = open('result.txt', 'w')
    result_file.write(DATABASE + "\n,")

    if width_of_peak > MAX_WIDTH:
        result_file.write("CONCLUDE: Time Gap is of the order of milliseconds. Probably underprovisioned network.")
        result_file.close()
    else:
        # Width of peak is of the order of microseconds. Possible microburst.
        result_set = mysql_manager.execute_query("select source_ip, count(hash) from packetrecords where switch = \'" + trigger_switch + "\' and time_in between " + str(left_time) + " and " + str(right_time) + " group by 1")
        
        data_points = []

        print("\nData points within the band:")
        for row in result_set[1:]:
            print("Flow:" + map_ip[row[0]] + " Count: " + str(row[1]))
            data_points.append(row[1])
        
        # Jain Fairness Index calculation
        # print(data_points)
        J_index = calculate_jain_index(data_points)

        n = len(data_points)
        print("\nTotal data points: " + str(sum(data_points)))
        
        normalized_J_index = (J_index - 1.0/n) / (1 - 1.0/n)
        result_file.write("Normalized J Index : " + str(normalized_J_index))

        writeConclusion(normalized_J_index, result_file)  

        result_file.close()      

    # Calculation of Ratios
    firstCall = True
    for switch in switch_map:
        print("Calculating ratios for switch " + str(switch_map[switch].identifier))
        getRatioTimeSeries(mysql_manager, switch_map[switch].identifier, DATABASE, firstCall)
        if firstCall:
            firstCall = False

    # Calculation of Instantaneous Throughput
    firstCall = True
    for switch in switch_map:
        result_set = mysql_manager.execute_query("select min(time_in), max(time_out) from packetrecords where switch = '"+ switch_map[switch].identifier + "'")
        left_cutoff = result_set[1:][0][0]
        right_cutoff = result_set[1:][0][1]
        if left_cutoff == None or right_cutoff == None:
            continue
        print("Calculating ingress throughput for switch " + str(switch_map[switch].identifier))
        getInstantaneousIngressThroughputTimeSeries(mysql_manager, switch_map[switch].identifier, DATABASE, firstCall)
        if firstCall:
            firstCall = False

    # Calculation of Paths
    print("Calculating Paths")
    getPaths(mysql_manager, DATABASE)

    # Re-establish connection since will now be accessing a table that
    # was newly created, hence schema in connection object needs to be 
    # updated.
    mysql_manager = MySQL_Manager(database=DATABASE,user=MYSQL_USER, password=MYSQL_PASSWORD)

    # Calculation of Egress throughputs
    firstCall = True
    for switch in switch_map:
        result_set = mysql_manager.execute_query("select min(time_exit), max(time_exit) from LINKMAPS where from_switch = '"+ switch_map[switch].identifier + "'")
        left_cutoff = result_set[1:][0][0]
        right_cutoff = result_set[1:][0][1]
        if left_cutoff == None or right_cutoff == None:
            continue
        print("Calculating egress throughput for switch " + str(switch_map[switch].identifier))
        getInstantaneousEgressThroughputTimeSeries(mysql_manager, switch_map[switch].identifier, DATABASE, firstCall)
        if firstCall:
            firstCall = False

    # Add a MySQL datasource to Grafana
    data_source = Grafana_Datasource(name=DATABASE, database_type="mysql", database=DATABASE)
    json_body = "{ " + data_source.get_json_string() + " }"
    resp = requests.request("POST", url=DATASOURCE_URL, headers=headers, data = json_body)
    print(resp)

def writeConclusion(normalizedJIndex, result_file):
    if normalizedJIndex > 0.7:
        result_file.write("\nCONCLUDE: It is probably a case of synchronized incast")
    elif normalizedJIndex < 0.45:
        result_file.write("\nCONCLUDE: It is probably a case of a dominant heavy hitter")
    else:
        result_file.write("\nCONCLUDE: Doesn't fall in either category")        

def calculate_jain_index(data_points):
    '''
    Calculates Jain's Fairness Index for values in a list data_points.
    Information on Jain's Fairness Index:
    https://en.wikipedia.org/wiki/Fairness_measure
    '''

    n = len(data_points)
    numerator = 0
    denominator = 0

    summation = sum(data_points)
    numerator = summation ** 2

    denominator = n *  sum( [x ** 2 for x in data_points] )

    J_index = numerator * 1.0 / denominator
    return J_index

def initializeSwitches(mysql_manager):
    '''
    Initializes a Switch object for each distinct switch in scenario
    and returns a mapping (dictionary) between switch ID and switch object.
    '''

    switchList = {}
    if mysql_manager is not None:
        result = mysql_manager.execute_query('select distinct switch from packetrecords')
        for row in result[1:]:
            switchList[row[0]] = Switch(identifier=row[0])
    
    return switchList

def initializeFlows(mysql_manager):
    '''
    Initializes a Flow object for each distinct flow in scenario
    and returns a mapping (dictionary) between source IP and Flow object.
    '''

    flowList = {}
    if mysql_manager is not None:
        result = mysql_manager.execute_query('select distinct source_ip from packetrecords')
        for row in result[1:]:
            flowList[row[0]] = Flow(identifier=row[0])
    
    return flowList

def getIpAddresses(mysql_manager):
    '''
    Purpose: IP addresses are stored as a 32 bit integer in the
    database. This function generates a mapping between 32 bit
    integer and human readable form of the IP, and returns a dictionary.
    '''

    result = mysql_manager.execute_query('select distinct source_ip from packetrecords')
    mapIp = {}

    for row in result[1:]:
        for ip in row:
            mapIp[ip] = Int2IP(ip)
    return mapIp

def get_formatted_time(year):
    '''
    Parameters: year : str
    Returns a string in the format accepted by Grafana
    '''

    return "{}-{}-{}".format(year, "01", "01")

def get_final_payload(dashboard):
    '''
    Parameters:
    dashboard : Object of type Dashboard

    Returns a string representing the final JSON object to be sent
    to Grafana for creating dashboard.
    '''

    payload = "{ \"dashboard\": {" + dashboard.get_json_string() + "}, \"overwrite\": true}"
    return payload
    
def getInstantaneousIngressThroughputTimeSeries(mysql_manager, switch, scenario, firstCall):
    '''
    Parameters: 
    mysql_manager : Object of MySQL_Manager class, used to communicate
    with MySQL instance.
    scenario : str -> the name of the scenario, also the database name
    in MySQL.
    switch : str -> Identifier of the switch for whom the time series
    of instantaneous ingress throughput needs to be calculated.
    firstCall : boolean -> If true, implies function is being called
    for the first time, hence appropriate table needs to be created,
    OR deleted if already existing and created again. firstCall is 
    passed on to insert function.
    '''

    result_set = mysql_manager.execute_query("select min(time_in), max(time_out) from packetrecords where switch = '"+ switch + "'")
    left_cutoff = result_set[1:][0][0]
    right_cutoff = result_set[1:][0][1]

    myDict = {}
    timeL, timeR = left_cutoff, left_cutoff + INTERVAL

    result_set = mysql_manager.execute_query("select source_ip, time_in from packetrecords where switch = '" + switch + "' order by time_in")[1:]

    currIndex = 0

    while timeL < right_cutoff:
        # Initialize frequency map
        freqMap = {}

        while currIndex < len(result_set) and result_set[currIndex][1] >= timeL and result_set[currIndex][1] <= timeR:
            # Add record
            ip =  result_set[currIndex][0]
            if ip not in freqMap:
                freqMap[ip] = 0
            freqMap[ip] += 1
            currIndex += 1
        
        # populate myDict
        for ip in freqMap:
            if ip not in myDict:
                myDict[ip] = []
            myDict[ip].append( (timeL, timeR, switch, freqMap[ip]) )
        timeL = timeR
        timeR = timeR + INTERVAL

    insertIngressThroughputIntoMySQL(myDict, scenario, switch, INTERVAL, firstCall)

def getInstantaneousEgressThroughputTimeSeries(mysql_manager, switch, scenario, firstCall):
    '''
    Parameters: 
    mysql_manager : Object of MySQL_Manager class, used to communicate
    with MySQL instance.
    scenario : str -> the name of the scenario, also the database name
    in MySQL.
    switch : str -> Identifier of the switch for whom the time series
    of instantaneous egress throughput needs to be calculated.
    firstCall : boolean -> If true, implies function is being called
    for the first time, hence appropriate table needs to be created,
    OR deleted if already existing and created again. firstCall is 
    passed on to insert function.
    '''
    
    result_set = mysql_manager.execute_query("select min(time_exit), max(time_exit) from LINKMAPS where from_switch = '"+ switch + "'")
    
    left_cutoff = result_set[1:][0][0]
    right_cutoff = result_set[1:][0][1]

    print(str(INTERVAL) + " nanosecs" )

    myDict = {}

    timeL, timeR = left_cutoff, left_cutoff + INTERVAL

    result_set = mysql_manager.execute_query("select to_switch, time_exit from LINKMAPS where from_switch = '" + switch + "' order by time_exit")[1:]
    
    currIndex = 0

    while timeL < right_cutoff:
        # Initialize frequency map
        freqMap = {}

        while currIndex < len(result_set) and result_set[currIndex][1] >= timeL and result_set[currIndex][1] <= timeR:
            # Add record
            to_switch =  result_set[currIndex][0]
            if to_switch not in freqMap:
                freqMap[to_switch] = 0
            freqMap[to_switch] += 1
            currIndex += 1
        
        # populate myDict
        for to_switch in freqMap:
            if to_switch not in myDict:
                myDict[to_switch] = []
            myDict[to_switch].append( (timeL, timeR, switch, freqMap[to_switch]) )
        timeL = timeR
        timeR = timeR + INTERVAL

    insertEgressThroughputIntoMySQL(myDict, scenario, switch, INTERVAL, firstCall)

def getRatioTimeSeries(mysql_manager, switch, scenario, firstCall):
    '''
    Parameters: 
    mysql_manager : Object of MySQL_Manager class, used to communicate
    with MySQL instance.
    scenario : str -> the name of the scenario, also the database name
    in MySQL.
    switch : str -> Identifier of the switch for whom the time series
    of ratio of flows needs to be calculated.
    firstCall : boolean -> If true, implies function is being called
    for the first time, hence appropriate table needs to be created,
    OR deleted if already existing and created again. firstCall is 
    passed on to insert function.
    '''

    result_set = mysql_manager.execute_query("select min(time_in), max(time_out) from packetrecords where switch = '"+ switch + "'")
    
    left_cutoff = result_set[1:][0][0]
    right_cutoff = result_set[1:][0][1]

    myDict = {}

    currTime = left_cutoff

    result_set = mysql_manager.execute_query("select source_ip, hash, time_in, time_out from packetrecords where switch = '" + switch + "' order by time_out")[1:]

    start_index = 0

    while currTime <= right_cutoff:
        IPFreq = {}

        for i in range(start_index, len(result_set)):
            row = result_set[i]
            if row[2] <= currTime and row[3] >= currTime:
                ip = row[0]
                if ip not in IPFreq:
                    IPFreq[ip] = 0
                IPFreq[ip] += 1
        

        totalPackets = sum([IPFreq[ip] for ip in IPFreq])
        
        # Update myDict
        for ip in IPFreq:
            if ip not in myDict:
                myDict[ip] = []
            myDict[ip].append( (currTime, 1.0 * IPFreq[ip] / totalPackets, totalPackets) )
        
        # Update start index
        while start_index < len(result_set) and result_set[start_index][3] < currTime:
            start_index += 1

        currTime += INTERVAL

    insertRatiosIntoMySQL(myDict, scenario, switch, firstCall)

def getPaths(mysql_manager, scenario):
    myDict = {}

    result_set = mysql_manager.execute_query("select time_in, time_out, switch, hash, source_ip from packetrecords order by hash, time_in")[1:]

    i = 0

    while i < len(result_set):

        rowList = []
        currRow = result_set[i]
        currHash = currRow[3]
        currIp = currRow[4]
        j = i
        while j < len(result_set) and result_set[j][3] == currHash:
            rowList.append(result_set[j])
            j += 1

        if len(rowList) == 1:
            i+=1
            continue

        i = j

        for k in range(len(rowList) - 1):
            fromSwitch = rowList[k][2]
            toSwitch = rowList[k+1][2]
            link = str(fromSwitch) + "-" + str(toSwitch)
            if link not in myDict:
                myDict[link] = []
            myDict[link].append([rowList[k][1], rowList[k+1][0], currHash, currIp])

    print("Inserting Paths")
    insertPathsIntoMySQL(myDict, scenario)

def insertPathsIntoMySQL(myDict, db_name):
    mysql_db = mysql.connector.connect(host="0.0.0.0", user="sankalp", passwd="sankalp123")
    mycursor = mysql_db.cursor()
    mycursor.execute("use " + db_name)

    mycursor.execute('DROP TABLE IF EXISTS LINKMAPS')
    mycursor.execute('CREATE TABLE LINKMAPS (time_enter bigint, time_exit bigint, from_switch VARCHAR(255), to_switch VARCHAR(255), hash bigint , source_ip bigint)')

    for link in myDict:
        query = 'INSERT INTO LINKMAPS (time_enter, time_exit, from_switch, to_switch, hash, source_ip) VALUES (%s, %s, %s, %s, %s, %s)'
        for [time_enter, time_exit, packetHash, source_ip] in myDict[link]:
            val = (time_enter, time_exit, link.split("-")[0], link.split("-")[1], packetHash, source_ip)
            mycursor.execute(query, val)
    
    mysql_db.commit()

def insertRatiosIntoMySQL(myDict, db_name, switch, firstCall):
    mysql_db = mysql.connector.connect(host="0.0.0.0", user="sankalp", passwd="sankalp123")
    mycursor = mysql_db.cursor()
    mycursor.execute("use " + db_name)

    if firstCall:
        mycursor.execute('DROP TABLE IF EXISTS RATIOS')
        mycursor.execute('CREATE TABLE RATIOS (time_stamp bigint, source_ip bigint, switch VARCHAR(255), ratio decimal(5,3) , total_pkts bigint)')

    for ip in myDict:
        query = 'INSERT INTO RATIOS (time_stamp, source_ip, switch, ratio, total_pkts) VALUES (%s, %s, %s, %s, %s)'
        for (timestamp, ratio, totalpkts) in myDict[ip]:
            val = (timestamp, ip, switch, ratio, totalpkts)
            mycursor.execute(query, val)

    mysql_db.commit()

def insertIngressThroughputIntoMySQL(myDict, db_name, switch, interval, firstCall):
    mysql_db = mysql.connector.connect(host="0.0.0.0", user="sankalp", passwd="sankalp123")
    mycursor = mysql_db.cursor()
    mycursor.execute("use " + db_name)

    if firstCall:
        mycursor.execute('DROP TABLE IF EXISTS THROUGHPUT')
        mycursor.execute('CREATE TABLE THROUGHPUT (time_in bigint, time_out bigint, source_ip bigint, switch VARCHAR(255), throughput decimal(7,3) )')

    for ip in myDict:
        query = 'INSERT INTO THROUGHPUT (time_in, time_out, source_ip, switch, throughput) VALUES (%s, %s, %s, %s, %s)'
        for (time_in, time_out, switch, throughput) in myDict[ip]:
            val = (time_in, time_out, ip, switch, (1.0 * throughput * 1500 * 8) / interval )
            mycursor.execute(query, val)

    mysql_db.commit()

def insertEgressThroughputIntoMySQL(myDict, db_name, switch, interval, firstCall):
    mysql_db = mysql.connector.connect(host="0.0.0.0", user="sankalp", passwd="sankalp123")
    mycursor = mysql_db.cursor()
    mycursor.execute("use " + db_name)

    if firstCall:
        mycursor.execute('DROP TABLE IF EXISTS EGRESSTHROUGHPUT')
        mycursor.execute('CREATE TABLE EGRESSTHROUGHPUT (time_in bigint, time_out bigint, from_switch VARCHAR(255), to_switch VARCHAR(255), throughput decimal(7,3) )')

    for to_switch in myDict:
        query = 'INSERT INTO EGRESSTHROUGHPUT (time_in, time_out, from_switch, to_switch, throughput) VALUES (%s, %s, %s, %s, %s)'
        for (time_in, time_out, from_switch, throughput) in myDict[to_switch]:
            val = (time_in, time_out, from_switch, to_switch, (1.0 * throughput * 1500 * 8) / interval )
            mycursor.execute(query, val)

    mysql_db.commit()

def Int2IP(ipnum):
    ''''
    Parameters:
    ipnum - a 32 bit integer that represents a traditional IP address

    Purpose: converts ipnum into a human readable string form.
    '''

    o1 = int(ipnum / pow(2,24)) % 256
    o2 = int(ipnum / pow(2,16)) % 256
    o3 = int(ipnum / pow(2,8)) % 256
    o4 = int(ipnum) % 256
    return '%(o1)s.%(o2)s.%(o3)s.%(o4)s' % locals()

if __name__ == "__main__":
    main()
