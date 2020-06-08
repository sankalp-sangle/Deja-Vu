import sys
import time

import mysql.connector
import requests
from mysql.connector import Error

from lib.config import (ANNOTATIONS_URL, API_KEY, CLEANUP_QUERIES, COLORS,
                        DATABASE, DATASOURCE_URL, HOST, LEFT_THRESHOLD,
                        MAX_LEGAL_UNIX_TIMESTAMP, MAX_WIDTH, RIGHT_THRESHOLD,
                        UNIX_TIME_START_YEAR, URL, YEAR_SEC, headers)
from lib.core import (Flow, Grafana_Dashboard, Grafana_Dashboard_Properties,
                      Grafana_Datasource, Grafana_Grid_Position, Grafana_Panel,
                      Grafana_Target, Grafana_Time, MySQL_Manager,
                      QueryBuilder, Switch)

flaggg1 = 0
flaggg2 = 0
flaggg3 = 0
flaggg4 = 0

def main():
    
    if len(sys.argv) != 2:
        print("Incorrect number of arguments\nUSAGE: $ python3 preprocess.py <scenario_name>")
        sys.exit(1)

    scenario = str(sys.argv[1])

    mysql_manager = MySQL_Manager(database=scenario)

    for query in CLEANUP_QUERIES:
        mysql_manager.execute_query(query)
    
    mapIp = getIpAddresses(mysql_manager)

    switchMap = initializeSwitches(mysql_manager)
    flowMap = initializeFlows(mysql_manager)

    for switch in switchMap:
        switchMap[switch].populateFlowList(mysql_manager)
        switchMap[switch].populateRatios(mysql_manager)
        switchMap[switch].print_info(mapIp)
        

    for flow in flowMap:
        flowMap[flow].populateSwitchList(mysql_manager)
        flowMap[flow].populateRatios(mysql_manager)
        flowMap[flow].print_info(mapIp) 

    # Get switch from which the trigger originated
    result = mysql_manager.execute_query('select switch from triggers')
    trigger_switch = result[1:][0][0]
    
    # Get peak queue depth
    result_set = mysql_manager.execute_query('select max(queue_depth) from packetrecords')
    peakDepth = result_set[1][0]
    print("Peak Depth: " + str(peakDepth))

    result_set = mysql_manager.execute_query('select time_in, time_out, queue_depth from packetrecords where queue_depth = ' + str(peakDepth))
    peakTimeIn = result_set[1][0]
    peakTimeOut = result_set[1][1]
    print("\nTime of peak depth: " + str(peakTimeOut))

    result_set = mysql_manager.execute_query('select time_in, time_out, queue_depth from packetrecords where switch = \'' + trigger_switch + '\' order by time_out')

    # Find index in result_set
    peakIndex = result_set.index( (peakTimeIn, peakTimeOut, peakDepth) )

    # Initialize left and right indices
    lIndex = peakIndex - 1
    rIndex = peakIndex + 1

    # Calculate left index
    while lIndex >= 0 and result_set[lIndex][2] > LEFT_THRESHOLD * peakDepth:
        lIndex = lIndex - 1

    # Calculate right index
    while rIndex < len(result_set) and result_set[rIndex][2] > RIGHT_THRESHOLD * peakDepth:
        rIndex = rIndex + 1

    # Ensure haven't gone beyond limits
    rIndex = (rIndex - 1) if rIndex == len(result_set) else rIndex
    lIndex = (lIndex + 1) if lIndex == 0 else lIndex

    lTime = result_set[lIndex][0]
    rTime = result_set[rIndex][1]

    timeDiff = rTime - lTime

    print("\nLeft time: {} Right time: {} Time difference: {} microseconds".format(str(lTime), str(rTime), str(timeDiff/1000)))

    if timeDiff > MAX_WIDTH:
        print("\nCONCLUDE: Time Gap is of the order of milliseconds. Probably underprovisioned network.")
    else:
        # Time gap is of the order of microseconds. Possible microburst.
        result_set = mysql_manager.execute_query("select source_ip, count(hash) from packetrecords where switch = \'" + trigger_switch + "\' and time_in between " + str(lTime) + " and " + str(rTime) + " group by 1")
        
        data_points = []

        print("\nData points within the band:")
        for row in result_set[1:]:
            print("Flow:" + mapIp[row[0]] + " Count: " + str(row[1]))
            data_points.append(row[1])
        
        # Jain Fairness Index calculation
        # print(data_points)
        J_index = calculate_jain_index(data_points)

        n = len(data_points)
        print("\nTotal data points: " + str(sum(data_points)))
        
        print("\nJ Index : " + str(J_index)) 
        normalizedJIndex = (J_index - 1.0/n) / (1 - 1.0/n)
        print("Normalized J Index : " + str(normalizedJIndex))

        printConclusion(normalizedJIndex)        

    trigger_time = mysql_manager.execute_query('select time_hit from triggers')[1][0]

    # Calculation of Ratios
    for switch in switchMap:
        result_set = mysql_manager.execute_query("select min(time_in), max(time_out) from packetrecords where switch = '"+ switchMap[switch].identifier + "'")
        left_cutoff = result_set[1:][0][0]
        right_cutoff = result_set[1:][0][1]
        print("Calculating ratios for " + str(switchMap[switch].identifier))
        getRatioTimeSeries(mysql_manager, switchMap[switch].identifier, (left_cutoff + right_cutoff) // 2, scenario)

    # Calculation of Instantaneous Throughput
    for switch in switchMap:
        result_set = mysql_manager.execute_query("select min(time_in), max(time_out) from packetrecords where switch = '"+ switchMap[switch].identifier + "'")
        left_cutoff = result_set[1:][0][0]
        right_cutoff = result_set[1:][0][1]
        print("Calculating instantaneous throughput for " + str(switchMap[switch].identifier))
        getInstantaneousThroughputTimeSeries(mysql_manager, switchMap[switch].identifier, (left_cutoff + right_cutoff) // 2, scenario)

    # Calculation of Paths
    getPaths(mysql_manager, scenario)

    mysql_manager = MySQL_Manager(database=scenario)

    # Calculation of Egress throughputs
    for switch in switchMap:
        result_set = mysql_manager.execute_query("select min(time_exit), max(time_exit) from linkmaps where from_switch = '"+ switchMap[switch].identifier + "'")
        left_cutoff = result_set[1:][0][0]
        right_cutoff = result_set[1:][0][1]
        if left_cutoff==None or right_cutoff==None:
            continue
        print("Calculating egress throughput for " + str(switchMap[switch].identifier))
        getInstantaneousEgressThroughputTimeSeries(mysql_manager, switchMap[switch].identifier, (left_cutoff + right_cutoff) // 2, scenario)


def printConclusion(normalizedJIndex):
    if normalizedJIndex > 0.7:
        print("\nCONCLUDE: It is probably a case of synchronized incast")
    elif normalizedJIndex < 0.45:
        print("\nCONCLUDE: It is probably a case of a dominant heavy hitter")
    else:
        print("\nCONCLUDE: Doesn't fall in either category")        

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
    return "{}-{}-{}".format(year, "01", "01")

def get_final_payload(dashboard):
    payload = "{ \"dashboard\": {" + dashboard.get_json_string() + "}, \"overwrite\": true}"
    return payload
    
def getInstantaneousThroughputTimeSeries(mysql_manager, switch, time, scenario):
    result_set = mysql_manager.execute_query("select min(time_in), max(time_out) from packetrecords where switch = '"+ switch + "'")
    # print(result_set)
    left_cutoff = result_set[1:][0][0]
    right_cutoff = result_set[1:][0][1]

    INTERVAL = (right_cutoff - left_cutoff) // 250

    myDict = {}

    timeL, timeR = left_cutoff, left_cutoff + INTERVAL

    while timeR < right_cutoff:
        result_set = mysql_manager.execute_query("select source_ip, count(hash) from packetrecords where time_in <= " + str(timeR) + " AND time_in >= " + str(timeL) + " and switch = '" + switch + "'" +  " GROUP BY source_ip")[1:]

        for row in result_set:
            if row[0] in myDict:
                myDict[row[0]].append( (timeL, timeR, switch, row[1]) )
            else:
                myDict[row[0]] = []
        
        timeL = timeR
        timeR = timeR + INTERVAL
    


    
    # for ip in myDict:
    #     print(str(ip))
    #     for tuple in myDict[ip]:
    #         print(str(ip) + " " + str(tuple))

    insertIntoSQL2(myDict, scenario, switch, INTERVAL)

def getInstantaneousEgressThroughputTimeSeries(mysql_manager, switch, time, scenario):
    result_set = mysql_manager.execute_query("select min(time_exit), max(time_exit) from linkmaps where from_switch = '"+ switch + "'")
    # print(result_set)
    left_cutoff = result_set[1:][0][0]
    right_cutoff = result_set[1:][0][1]

    INTERVAL = (right_cutoff - left_cutoff) // 500

    print(str(INTERVAL) + " nanosecs" )

    myDict = {}

    timeL, timeR = left_cutoff, left_cutoff + INTERVAL

    while timeR < right_cutoff:
        result_set = mysql_manager.execute_query("select to_switch, count(hash) from linkmaps where time_exit <= " + str(timeR) + " AND time_exit >= " + str(timeL) + " and from_switch = '" + switch + "'" +  " GROUP BY to_switch")[1:]

        for row in result_set:
            if row[0] in myDict:
                myDict[row[0]].append( (timeL, timeR, switch, row[1]) )
            else:
                myDict[row[0]] = []
        
        timeL = timeR
        timeR = timeR + INTERVAL
    
    insertIntoSQL4(myDict, scenario, switch, INTERVAL)

def getRatioTimeSeries(mysql_manager, switch, time, scenario):
    result_set = mysql_manager.execute_query("select min(time_in), max(time_out) from packetrecords where switch = '"+ switch + "'")
    # print(result_set)
    left_cutoff = result_set[1:][0][0]
    right_cutoff = result_set[1:][0][1]

    INTERVAL = (right_cutoff - left_cutoff) // 250

    myDict = {}

    leftPointer = time
    while leftPointer > left_cutoff:
        result_set = mysql_manager.execute_query("select source_ip, count(hash) from packetrecords where time_in < " + str(leftPointer) + " AND time_out > " + str(leftPointer) + " and switch = '" + switch + "'" +  " GROUP BY source_ip")[1:]
        totalPackets = sum([row[1] for row in result_set])
        print("Total pkts at time " + str(leftPointer) + " is " + str(totalPackets))
        for row in result_set:
            if row[0] in myDict:
                myDict[row[0]].append( (leftPointer, 1.0 * row[1] / totalPackets, totalPackets) )
            else:
                myDict[row[0]] = []

        leftPointer = leftPointer - INTERVAL

    rightPointer = time + INTERVAL    
    while rightPointer < right_cutoff:
        result_set = mysql_manager.execute_query("select source_ip, count(hash) from packetrecords where time_in < " + str(rightPointer) + " AND time_out > " + str(rightPointer) +  " and switch = '" + switch + "'" +  " GROUP BY source_ip")[1:]
        totalPackets = sum([row[1] for row in result_set])
        print("Total pkts at time " + str(rightPointer) + " is " + str(totalPackets))
        for row in result_set:
            if row[0] in myDict:
                myDict[row[0]].append( (rightPointer, 1.0 * row[1] / totalPackets, totalPackets) )
            else:
                myDict[row[0]] = []

        rightPointer = rightPointer + INTERVAL
            
    
    # for ip in myDict:
    #     print(str(ip))
    #     for tuple in myDict[ip]:
    #         print(str(ip) + " " + str(tuple))

    insertIntoSQL(myDict, scenario, switch)

def getPaths(mysql_manager, scenario):
    myDict = {}
    # print(len(packetHashes))

    result_set = mysql_manager.execute_query("select time_in, time_out, switch, hash from packetrecords order by hash, time_in")[1:]

    i = 0
    print(len(result_set))
    while i < len(result_set):
        print(i)

        rowList = []
        currRow = result_set[i]
        currHash = currRow[3]
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
            myDict[link].append([rowList[k][1], rowList[k+1][0], currHash])

    print("Here's the dict:")
    print(myDict)
    insertIntoSQL3(myDict, scenario)

def insertIntoSQL3(myDict, db_name):
    global flaggg3
    mysql_db = mysql.connector.connect(host="0.0.0.0", user="sankalp", passwd="sankalp")
    mycursor = mysql_db.cursor()
    mycursor.execute("use " + db_name)

    if flaggg3 == 0:
        mycursor.execute('DROP TABLE IF EXISTS LINKMAPS')
        flaggg3 = 1
        mycursor.execute('CREATE TABLE LINKMAPS (time_enter bigint, time_exit bigint, from_switch VARCHAR(255), to_switch VARCHAR(255), hash bigint )')

    for link in myDict:
        query = 'INSERT INTO LINKMAPS (time_enter, time_exit, from_switch, to_switch, hash) VALUES (%s, %s, %s, %s, %s)'
        for [time_enter, time_exit, packetHash] in myDict[link]:
            val = (time_enter, time_exit, link.split("-")[0], link.split("-")[1], packetHash)
            mycursor.execute(query, val)
    
    mysql_db.commit()

def insertIntoSQL(myDict, db_name, switch):
    global flaggg1
    mysql_db = mysql.connector.connect(host="0.0.0.0", user="sankalp", passwd="sankalp")
    mycursor = mysql_db.cursor()
    mycursor.execute("use " + db_name)

    if flaggg1 == 0:
        mycursor.execute('DROP TABLE IF EXISTS RATIOS')
        flaggg1 = 1
        mycursor.execute('CREATE TABLE RATIOS (time_stamp bigint, source_ip bigint, switch VARCHAR(255), ratio decimal(5,3) , total_pkts bigint)')

    for ip in myDict:
        query = 'INSERT INTO RATIOS (time_stamp, source_ip, switch, ratio, total_pkts) VALUES (%s, %s, %s, %s, %s)'
        for (timestamp, ratio, totalpkts) in myDict[ip]:
            val = (timestamp, ip, switch, ratio, totalpkts)
            mycursor.execute(query, val)

    mysql_db.commit()

def insertIntoSQL2(myDict, db_name, switch, interval):
    global flaggg2
    mysql_db = mysql.connector.connect(host="0.0.0.0", user="sankalp", passwd="sankalp")
    mycursor = mysql_db.cursor()
    mycursor.execute("use " + db_name)

    if flaggg2 == 0:
        mycursor.execute('DROP TABLE IF EXISTS THROUGHPUT')
        flaggg2 = 1
        mycursor.execute('CREATE TABLE THROUGHPUT (time_in bigint, time_out bigint, source_ip bigint, switch VARCHAR(255), throughput decimal(7,3) )')

    for ip in myDict:
        query = 'INSERT INTO THROUGHPUT (time_in, time_out, source_ip, switch, throughput) VALUES (%s, %s, %s, %s, %s)'
        for (time_in, time_out, switch, throughput) in myDict[ip]:
            val = (time_in, time_out, ip, switch, (1.0 * throughput * 1500 * 8) / interval )
            mycursor.execute(query, val)

    mysql_db.commit()

def insertIntoSQL4(myDict, db_name, switch, interval):
    global flaggg4
    mysql_db = mysql.connector.connect(host="0.0.0.0", user="sankalp", passwd="sankalp")
    mycursor = mysql_db.cursor()
    mycursor.execute("use " + db_name)

    if flaggg4 == 0:
        mycursor.execute('DROP TABLE IF EXISTS EGRESSTHROUGHPUT')
        flaggg4 = 1
        mycursor.execute('CREATE TABLE EGRESSTHROUGHPUT (time_in bigint, time_out bigint, from_switch VARCHAR(255), to_switch VARCHAR(255), throughput decimal(7,3) )')

    for to_switch in myDict:
        query = 'INSERT INTO EGRESSTHROUGHPUT (time_in, time_out, from_switch, to_switch, throughput) VALUES (%s, %s, %s, %s, %s)'
        for (time_in, time_out, from_switch, throughput) in myDict[to_switch]:
            val = (time_in, time_out, from_switch, to_switch, (1.0 * throughput * 1500 * 8) / interval )
            mycursor.execute(query, val)

    mysql_db.commit()

def Int2IP(ipnum):
    o1 = int(ipnum / pow(2,24)) % 256
    o2 = int(ipnum / pow(2,16)) % 256
    o3 = int(ipnum / pow(2,8)) % 256
    o4 = int(ipnum) % 256
    return '%(o1)s.%(o2)s.%(o3)s.%(o4)s' % locals()

if __name__ == "__main__":
    main()
