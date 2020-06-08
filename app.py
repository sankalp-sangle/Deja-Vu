import json
import os
from collections import deque

import mysql.connector
import requests
from flask import (Flask, g, redirect, render_template, request, session,
                   url_for)
from flask_bootstrap import Bootstrap
from mysql.connector import Error

from lib.core import (Grafana_Dashboard, Grafana_Dashboard_Properties, Flow, Grafana_Grid_Position,
                      MySQL_Manager, Grafana_Panel, QueryBuilder, Switch, Grafana_Target, Grafana_Time, Scenario)
from lib.forms import PacketSearchForm, QueryForm, RandomQuery, SimpleButton
from lib.config import (ANNOTATIONS_URL, API_KEY, COLORS, DATABASE,
                      DATASOURCE_URL, HOST, UNIX_TIME_START_YEAR, URL,
                      YEAR_SEC, MAX_LEGAL_UNIX_TIMESTAMP, headers)

# Global declarations

app = Flask(__name__)
app.config['SECRET_KEY'] = "Gangadhar hi Shaktimaan hai"

bootstrap = Bootstrap(app)

scenario = Scenario()

mysql_manager = MySQL_Manager(database=DATABASE)

# End of Global declarations


@app.errorhandler(404)
def page_not_found(e):
    '''
    Purpose: Function handler for a request that triggers 404 response.
    '''
    return render_template('404.html'), 404


@app.errorhandler(500)
def internal_server_error(e):
    '''
    Purpose: Function handler for a request that triggers 500 response.
    '''
    return render_template('500.html'), 500


@app.route('/')
def index():
    return render_template('index.html', switch=scenario.trigger_switch, duration=(scenario.max_time-scenario.min_time) / 10**9)

@app.route('/switches')
def switches():
    lvlToSwitch = {}
    for key in scenario.switch_to_level_mapping:
        if scenario.switch_to_level_mapping[key] in lvlToSwitch:
            lvlToSwitch[scenario.switch_to_level_mapping[key]].append(key)
        else:
            lvlToSwitch[scenario.switch_to_level_mapping[key]] = [key]

    other_switches = scenario.all_switches - scenario.tor_switches
    return render_template('switches.html', lvlToSwitch = lvlToSwitch, tor_switches=sorted(list(scenario.tor_switches)), other_switches=sorted(list(other_switches)) , trigger_switch = scenario.trigger_switch)

@app.route('/switches/<switch>', methods=['GET', 'POST'])
def displaySwitch(switch):
    global scenario
    form = SimpleButton()

    points = []
    for flow in scenario.switch_arr[switch].flow_list:
        entry = {}
        entry['SourceIP'] = scenario.map_ip[flow]
        entry['Ratio'] = scenario.switch_arr[switch].ratios[flow]
        points.append(entry)

    if request.method == "GET":
        return render_template('switchinfo.html', points=points, mapIp=scenario.map_ip, switch=scenario.switch_arr[switch], form=form, level = scenario.switch_to_level_mapping[switch])
    elif request.method == "POST":
        if form.validate_on_submit():

            panelList = getPanels(g.mysql_manager, switch)

            time_from_seconds = g.mysql_manager.execute_query('select min(time_in) from packetrecords where switch = ' + switch)[1][0]
            time_to_seconds = g.mysql_manager.execute_query('select max(time_out) from packetrecords where switch = ' + switch)[1][0]

            print(time_from_seconds)
            print(time_to_seconds)

            year_from = UNIX_TIME_START_YEAR + int( (time_from_seconds * MAX_LEGAL_UNIX_TIMESTAMP / scenario.max_time) // YEAR_SEC)
            year_to = UNIX_TIME_START_YEAR + 1 + int( (time_to_seconds * MAX_LEGAL_UNIX_TIMESTAMP / scenario.max_time) // YEAR_SEC)

            print(year_from)
            print(year_to)
            
            time_from = getFormattedTime(year_from)
            time_to = getFormattedTime(year_to)

            dashboard = Grafana_Dashboard(properties=Grafana_Dashboard_Properties(title="Switch " + switch ,time=Grafana_Time(timeFrom=time_from, timeTo=time_to)), panels=panelList)
            
            payload = getFinalPayload(dashboard)
            response = requests.request("POST", url=URL, headers=headers, data = payload)
            dashboardUId = response.json()['uid']
            dashboardId = response.json()['id']

            # Delete existing annotations
            response = requests.request("GET", url=ANNOTATIONS_URL, headers=headers)
            annotations = response.json()
            for annotation in annotations:
                annotationId = annotation['id']
                response = requests.request("DELETE", url=ANNOTATIONS_URL + "/" + str(annotationId), headers=headers)

            # Post annotations
            trigger_time = g.mysql_manager.execute_query('select time_hit from triggers')[1][0]
            annotations_payload = "{ \"time\":" + str(int(trigger_time * MAX_LEGAL_UNIX_TIMESTAMP / scenario.max_time)) + "000" + ", \"text\":\"Trigger Hit!\", \"dashboardId\":" + str(dashboardId) + "}"
            print(annotations_payload)
            response = requests.request("POST", url=ANNOTATIONS_URL, headers=headers, data = annotations_payload)

            return render_template('switchinfo.html', points = points, mapIp=scenario.map_ip, switch=scenario.switch_arr[switch], form = form, dashboardID=dashboardUId, level = scenario.switch_to_level_mapping[switch])

@app.route('/query', methods=['GET', 'POST'])
def query():
    form = QueryForm()
    form2 = RandomQuery()
    results = None
    if form.submit1.data and form.validate_on_submit():
        q = QueryBuilder(time_column=form.time.data, value = form.value.data, metricList=form.metric.data.split(",")).get_generic_query() + " LIMIT 10000"
        results = g.mysql_manager.execute_query(q)
    elif form2.submit2.data and form2.validate_on_submit():
        results = g.mysql_manager.execute_query(form2.query.data)
    return render_template('query.html', form=form, form2 = form2, results=results)

@app.route('/packetwise', methods=['GET', 'POST'])
def packetwise():

    form = PacketSearchForm()
    form2 = SimpleButton()
    results = None
    
    if request.method == "GET":
        return render_template('packetwise.html', form=form, form2=form2, results=results)
    
    elif request.method == "POST":
        if form.validate_on_submit():
            q = "select time_in, switch from packetrecords where hash = " + form.hash.data + " order by time_in"
            results = g.mysql_manager.execute_query(q)
            results = results[1:]
            session['hash'] = form.hash.data
            session['results'] = results
            return render_template('packetwise.html', form=form, form2=form2, results=results)
        elif form2.validate_on_submit():
            hash = form.hash.data
            
            time_from_seconds = g.mysql_manager.execute_query('select min(time_in) from packetrecords where hash = ' + session.get('hash'))[1][0]
            time_to_seconds = g.mysql_manager.execute_query('select max(time_out) from packetrecords where hash = ' + session.get('hash'))[1][0]

            year_from = UNIX_TIME_START_YEAR + (time_from_seconds * MAX_LEGAL_UNIX_TIMESTAMP / scenario.max_time) // YEAR_SEC
            year_to = UNIX_TIME_START_YEAR + 1 + (time_to_seconds * MAX_LEGAL_UNIX_TIMESTAMP / scenario.max_time) // YEAR_SEC
            
            time_from = getFormattedTime(year_from)
            time_to = getFormattedTime(year_to)

            q = "select time_in as \'time\', concat(switch) as metric, time_queue from packetrecords where hash = " + session.get('hash') + " order by time_in"

            panelList = []
            panelList.append(Grafana_Panel(title="Packet " + session.get('hash'), targets = [Grafana_Target(rawSql=q)], datasource=DATABASE))

            dashboard = Grafana_Dashboard(properties=Grafana_Dashboard_Properties(title="Packet " + hash ,time=Grafana_Time(timeFrom=time_from, timeTo=time_to)), panels=panelList)
            payload = getFinalPayload(dashboard)

            response = requests.request("POST", url=URL, headers=headers, data = payload)

            dashboardId = response.json()['uid']
    
            return render_template('packetwise.html', form=PacketSearchForm(), form2=form2, results = session.get('results'), dashboardId=dashboardId)
    

@app.route('/flows/<flow>')
def displayFlow(flow):

    if "." in flow:
        flow = str(IP2Int(flow))
    res = g.mysql_manager.execute_query("select distinct source_ip from packetrecords")[1:]
    choices = [row[0] for row in res]
    lvlToSwitch = {}
    for switch in scenario.flow_arr[int(flow)].switch_list:
        if scenario.switch_to_level_mapping[switch] in lvlToSwitch:
            lvlToSwitch[scenario.switch_to_level_mapping[switch]].append(switch)
        else:
            lvlToSwitch[scenario.switch_to_level_mapping[switch]] = [switch]
    res = g.mysql_manager.execute_query("select switch, min(time_in) from packetrecords where source_ip=\'"+ flow + "\' group by switch")[1:]
    time_in_list = [int(row[1]) for row in res]

    interval = max(time_in_list) - min(time_in_list)

    minLim = str(min(time_in_list) - 0.25 * interval)
    maxLim = str(max(time_in_list) + 0.25 * interval)
    minLim2 = {"val":min(time_in_list) - 0.25 * interval }
    maxLim2 = {"val":max(time_in_list) + 0.25 * interval }
    times={}
    for row in res:
        times[row[0]] = int(row[1])

    return render_template('flowinfo.html', mapIp=scenario.map_ip, flo = scenario.flow_arr[int(flow)], lvlToSwitch=lvlToSwitch, nodelist=scenario.nodelist, linklist=scenario.linklist, times=times, maxLim=maxLim, minLim = minLim, minLim2 = minLim2, maxLim2=maxLim2, choices = choices)

@app.route('/flows')
def flows():
    return render_template('flows.html', mapIp=scenario.map_ip, flows = scenario.flow_arr, switches = scenario.switch_arr, trigger_switch=scenario.trigger_switch)

@app.route('/grafana')
def grafana():
    return render_template('grafana.html')

@app.route('/allflows', methods=["POST"])
def allflows():
    requestedFlows = []
    for key in request.form:
        requestedFlows.append(int(key))

    times=[]

    smallestTime = 2**40
    largestTime = 0

    for flow in requestedFlows:
        res = g.mysql_manager.execute_query("select switch, min(time_in) from packetrecords where source_ip=\'"+ str(scenario.flow_arr[flow].identifier) + "\' group by switch")[1:]
        timmes = {}
        for row in res:
            timmes[row[0]] = int(row[1])

        smallestTime = min(smallestTime, min([int(row[1]) for row in res]))
        largestTime = max(largestTime, max([int(row[1]) for row in res]))
        
        times.append(timmes)

    interval = largestTime - smallestTime
    minLim = str( max(smallestTime - 0.1 * interval, 0) )
    maxLim = str(largestTime + 0.1 * interval)
    minLim2 = {"val":max(smallestTime - 0.1 * interval, 0)}
    maxLim2 = {"val":largestTime + 0.1 * interval}


    return render_template('allflows.html', mapIp = scenario.map_ip, flowIPS=requestedFlows, noOfFlows=len(requestedFlows), noOfFlowsJS = {"val":len(requestedFlows)}, nodelist=scenario.nodelist, linklist=scenario.linklist, times=times, maxLim=maxLim, minLim = minLim, minLim2 = minLim2, maxLim2=maxLim2)

@app.before_request
def before_request():
    '''
    Code in this function is run before every request
    '''
    g.mysql_manager = MySQL_Manager(database=DATABASE)

@app.route('/topo')
def topo():
    return render_template("topology.html", nodelist = scenario.nodelist, linklist = scenario.linklist, trigger_switch = scenario.trigger_switch)

@app.route('/general')
def general():

    throughputlimits = {}
    res3 = g.mysql_manager.execute_query('select from_switch, to_switch, min(time_out), max(time_out) from egressthroughput group by 1,2')[1:]
    for row in res3:
        entry = {}
        entry['min'] = row[2]
        entry['max'] = row[3]
        throughputlimits[str(row[0]) + "-" + str(row[1])] = entry


    res = g.mysql_manager.execute_query('select time_out, from_switch, to_switch, throughput, time_in from egressthroughput order by time_out')[1:]
    throughput = {}
    for row in res:
        if str(row[1]) + "-" + str(row[2]) in throughput:
            entry2 = {}
            entry2['time_out'] = ( row[0] + row[4] ) // 2
            entry2['throughput'] = str(row[3])
            throughput[str(row[1]) + "-" + str(row[2])].append(entry2)
        else:
            throughput[str(row[1]) + "-" + str(row[2])] = []
            entry2 = {}
            entry2['time_out'] = ( row[0] + row[4] ) // 2
            entry2['throughput'] = str(row[3])
            throughput[str(row[1]) + "-" + str(row[2])].append(entry2)



    res = g.mysql_manager.execute_query('select switch, time_out, queue_depth * 80 DIV 1500 from packetrecords order by time_out')[1:]
    datas = {}
    for row in res:
        if row[0] in datas:
            entry = {}
            entry['time_out'] = row[1]
            entry['queue_depth'] = row[2]
            datas[row[0]].append(entry)
        else:
            datas[row[0]] = []
            entry = {}
            entry['time_out'] = row[1]
            entry['queue_depth'] = row[2]
            datas[row[0]].append(entry)

    limits = {}
    res3 = g.mysql_manager.execute_query('select switch, min(time_out), max(time_out) from packetrecords group by switch')[1:]
    for row in res3:
        entry = {}
        entry['min'] = row[1]
        entry['max'] = row[2]
        limits[row[0]] = entry

    res1 = g.mysql_manager.execute_query('select min(time_out) from egressthroughput')[1:]
    res2 = g.mysql_manager.execute_query('select max(time_out) from egressthroughput')[1:]

    interval = res2[0][0] - res1[0][0]
    
    minLim = str(res1[0][0])
    maxLim = str(res2[0][0])
    minLim2 = {"val":res1[0][0]}
    maxLim2 = {"val":res2[0][0]}

    maxDepth = g.mysql_manager.execute_query('select max(queue_depth) from packetrecords')[1:][0][0]
    triggerTime = g.mysql_manager.execute_query('select time_hit from triggers')[1:][0][0]
    triggerNode = g.mysql_manager.execute_query('select switch from triggers')[1:][0][0]

    return render_template('general.html', triggerNode = {"id":triggerNode}, triggerTime = {"val":triggerTime}, levels = scenario.switch_to_level_mapping, maxD = {"val":(maxDepth * 80) // 1500}, throughputlimits=throughputlimits, nodelist=scenario.nodelist, throughput = throughput, linklist=scenario.linklist, limits = limits, datas=datas, maxLim=maxLim, minLim = minLim, minLim2 = minLim2, maxLim2=maxLim2)

def getFinalPayload(dashboard):
    '''
    Parameters:
    dashboard : Object of type Dashboard

    Returns a string representing the final JSON object to be sent
    to Grafana for creating dashboard.
    '''

    payload = "{ \"dashboard\": {" + dashboard.get_json_string() + "}, \"overwrite\": true}"
    return payload

def getFormattedTime(year):
    '''
    Parameters: year : str
    Returns a string in the format accepted by Grafana
    '''
    
    return "{}-{}-{}".format(year, "01", "01")

def generateTopologyGraph():
    '''
    Purpose: Construct an adjacency list representation of the graph
    that models the topology of switches in the data center.
    Info on adjacency list representation:
    https://www.geeksforgeeks.org/graph-and-its-representations/
    '''
    global scenario

    links = mysql_manager.execute_query("select switch1, switch2, cap from links")[1:]
    for link in links:
        u, v, cap = link[0], link[1], link[2]
        if u in scenario.topology_graph:
            scenario.topology_graph[u].append([v, cap])
        else:
            scenario.topology_graph[u] = []
            scenario.topology_graph[u].append([v, cap])

def getSwitchToLevelMapping():
    '''
    Purpose: Generate a mapping between switch identifier and the
    level of the switch.
    Top of Rack switches are at level 1, aggregates are at level
    two, and cores are at level 3.
    Algorithm: Perform a Breadth First Search from each top of rack
    switches, and calculate the distance from root node. Assign to
    each node the minimum of the current distance from root node
    and the existing value of the distance(if any).
    '''
    global scenario
    for tor in scenario.tor_switches:
        visited = []
        # Using a Deque data structure to maintain the queue for 
        # Breadth-first search
        bfs(visited, deque([[tor, 1]]))
        
def bfs(visited, queue):
    '''
    Purpose: Recursive implementation of breadth first search. Pops out
    the switch at front of the queue, assigns a level to it, and adds
    all its neighbours that are not visited yet to the queue.
    '''
    global scenario
    if len(queue) == 0:
        return
    
    node = queue.popleft()
    if node[0] in scenario.switch_to_level_mapping:
        scenario.switch_to_level_mapping[node[0]] = min(scenario.switch_to_level_mapping[node[0]], node[1])
    else:
        scenario.switch_to_level_mapping[node[0]] = node[1]
    for adj in scenario.topology_graph[node[0]]:
        if adj[0] not in visited:
            visited.append(adj[0])
            queue.append([adj[0], node[1] + 1])
    
    bfs(visited, queue)

def getTopologyPositions():
    '''
    Purpose: Pending
    '''
    global scenario
    BEGINX = 20
    BEGINY = 70
    nodesAddedToLevel = {}
    for node in scenario.switch_to_level_mapping:
        nodesAddedToLevel[scenario.switch_to_level_mapping[node]] = []

    for node in scenario.topology_graph:
        nodeEntry = {}
        nodeEntry["id"] = node
        nodeEntry["group"] = "switch"
        nodeEntry["label"] = node
        nodeEntry["level"] = scenario.switch_to_level_mapping[node]
        nodesAddedToLevel[scenario.switch_to_level_mapping[node]].append(node)
        nodeEntry["x"] = BEGINX + 200 * len(nodesAddedToLevel[scenario.switch_to_level_mapping[node]])
        nodeEntry["y"] = BEGINY + (3-scenario.switch_to_level_mapping[node]) * 200
        scenario.nodelist.append(nodeEntry)
    
    links = mysql_manager.execute_query("select * from links")[1:]

    for link in links:
        linkEntry = {}
        linkEntry["source"] = link[0]
        linkEntry["target"] = link[1]
        linkEntry["strength"] = 0.7
        scenario.linklist.append(linkEntry)

def Int2IP(ipnum: int) -> str:
    '''
    Parameters:
    ipnum - a 32 bit integer that represents a traditional IP address

    Purpose: converts ipnum into a human readable string form.
    '''
    
    o1 = int(ipnum / pow(2,24)) % 256
    o2 = int(ipnum / pow(2,16)) % 256
    o3 = int(ipnum / pow(2,8)) % 256
    o4 = int(ipnum) % 256
    return '%(o1)s.%(o2)s.%(o3)s.%(o4)s' % locals()

def IP2Int(ip: str) -> int:
    '''
    Parameters:
    ip: the human readable string representation of an IP address

    Purpose: converts ip into a 32 bit integer.
    '''

    o = list(map(int, ip.split('.')))
    res = (16777216 * o[0]) + (65536 * o[1]) + (256 * o[2]) + o[3]
    return res

def getIPAddressMapping():
    '''
    Purpose: IP addresses are stored as a 32 bit integer in the
    database. This function generates a mapping between 32 bit
    integer and human readable form of the IP.
    '''

    global scenario
    result = mysql_manager.execute_query('select distinct source_ip from packetrecords')

    for row in result[1:]:
        for ip in row:
            scenario.map_ip[ip] = Int2IP(ip)

def getPanels(mysql_manager, switch):
    '''
    Purpose: Pending
    '''
    global scenario
    panelList = []
    colorMap = {}

    indexOfAvailableColour = 0
    
    q1 = QueryBuilder(time_column = "time_stamp * " + str(MAX_LEGAL_UNIX_TIMESTAMP) + " / " + str(scenario.max_time), value= 'ratio', metricList = ['switch', 'source_ip'],  table='ratios', isConditional=True, conditionalClauseList=['switch = \'' + str(switch) + '\'']).get_generic_query()
    aliasColors, indexOfAvailableColour, colorMap = getAliasColors(mysql_manager, q1, colorMap, indexOfAvailableColour)
    panelList.append(Grafana_Panel(gridPos=Grafana_Grid_Position(x=0,y=0), title="Default Panel: Relative ratios of packets for each flow at Switch " + switch, targets = [Grafana_Target(rawSql=q1)], datasource=DATABASE, aliasColors=aliasColors))
    
    q2 = QueryBuilder(time_column = "time_stamp * " + str(MAX_LEGAL_UNIX_TIMESTAMP) + " / " + str(scenario.max_time), value= 'ratio*total_pkts', metricList = ['switch', 'source_ip'],  table='ratios', isConditional=True, conditionalClauseList=['switch = \'' + str(switch) + '\'']).get_generic_query()
    aliasColors, indexOfAvailableColour, colorMap = getAliasColors(mysql_manager, q2, colorMap, indexOfAvailableColour)
    panelList.append(Grafana_Panel(gridPos=Grafana_Grid_Position(x=0,y=11), title="Default Panel: Relative ratios of packets for each flow at Switch " + switch, targets = [Grafana_Target(rawSql=q2)], datasource=DATABASE, lines = False, bars = True, stack = True, percentage = False, aliasColors=aliasColors))

    q3 = QueryBuilder(time_column = "time_in * " + str(MAX_LEGAL_UNIX_TIMESTAMP) + " / " + str(scenario.max_time), value = 'throughput', metricList = ['from_switch','to_switch'], table='egressthroughput', isConditional=True, conditionalClauseList=['from_switch = \'' + str(switch) + '\'']).get_generic_query()
    aliasColors, indexOfAvailableColour, colorMap = getAliasColors(mysql_manager, q3, colorMap, indexOfAvailableColour)
    panelList.append(Grafana_Panel(gridPos=Grafana_Grid_Position(x=12,y=22),title="Default Panel: Instantaneous Egress Throughput at Switch " + switch, targets = [Grafana_Target(rawSql=q3)], datasource=DATABASE, aliasColors=aliasColors))

    q4 = QueryBuilder(time_column = "time_in * " + str(MAX_LEGAL_UNIX_TIMESTAMP) + " / " + str(scenario.max_time), value = 'link_utilization', metricList = ['switch', 'source_ip'], isConditional=True, conditionalClauseList=['switch = \'' + str(switch) + '\'']).get_generic_query()
    aliasColors, indexOfAvailableColour, colorMap = getAliasColors(mysql_manager, q4, colorMap, indexOfAvailableColour)
    panelList.append(Grafana_Panel(gridPos=Grafana_Grid_Position(x=0,y=33),title="Default Panel: Link Utilization", targets = [Grafana_Target(rawSql=q4)], datasource=DATABASE, aliasColors=aliasColors))

    q5 = QueryBuilder(time_column = "time_in * " + str(MAX_LEGAL_UNIX_TIMESTAMP) + " / " + str(scenario.max_time), value = 'source_ip % 10', metricList = ['switch', 'source_ip'], isConditional=True, conditionalClauseList=['switch = \'' + str(switch) + '\'']).get_generic_query()
    aliasColors, indexOfAvailableColour, colorMap = getAliasColors(mysql_manager, q5, colorMap, indexOfAvailableColour)
    panelList.append(Grafana_Panel(gridPos=Grafana_Grid_Position(x=12,y=0),title="Packet distribution at trigger switch", targets = [Grafana_Target(rawSql=q5)], datasource=DATABASE, points = True, lines = False, aliasColors=aliasColors))
    
    q6 = QueryBuilder(time_column = "time_in * " + str(MAX_LEGAL_UNIX_TIMESTAMP) + " / " + str(scenario.max_time), value = 'queue_depth', metricList = ['switch'], isConditional=True, conditionalClauseList=['switch = \'' + str(switch) + '\'']).get_generic_query()
    aliasColors, indexOfAvailableColour, colorMap = getAliasColors(mysql_manager, q6, colorMap, indexOfAvailableColour)
    panelList.append(Grafana_Panel(gridPos=Grafana_Grid_Position(x=12,y=11),title="Default Panel: Queue Depth", targets = [Grafana_Target(rawSql=q6)], datasource=DATABASE, aliasColors=aliasColors))

    q7 = QueryBuilder(time_column = "time_in * " + str(MAX_LEGAL_UNIX_TIMESTAMP) + " / " + str(scenario.max_time), value = 'throughput', metricList = ['switch', 'source_ip'], table='throughput', isConditional=True, conditionalClauseList=['switch = \'' + str(switch) + '\'']).get_generic_query()
    aliasColors, indexOfAvailableColour, colorMap = getAliasColors(mysql_manager, q7, colorMap, indexOfAvailableColour)
    panelList.append(Grafana_Panel(gridPos=Grafana_Grid_Position(x=12,y=22),title="Default Panel: Instantaneous Ingress Throughput at Switch " + switch, targets = [Grafana_Target(rawSql=q7)], datasource=DATABASE, aliasColors=aliasColors))

    return panelList

def getAliasColors(mysql_manager, q, colorMap, indexOfAvailableColour):
    '''
    Purpose: Pending
    '''    
    result_set = mysql_manager.execute_query(q)[1:]
    metricSet = set([row[1] for row in result_set])
    aliasColors = ""
    for metric in metricSet:
        if metric not in colorMap:
            colorMap[metric] = COLORS[indexOfAvailableColour]
            indexOfAvailableColour += 1
            indexOfAvailableColour %= len(COLORS) 
        aliasColors += '\"' + metric + '\":\"' + colorMap[metric] + '\",'
    aliasColors = aliasColors[:-1]
    return aliasColors, indexOfAvailableColour, colorMap

def generateScenarioData():
    ''' 
    Purpose: Populate the globally declared scenario object of type Scenario
    that is designed to contain all data about the scenario being viewed
    currently.
    '''
    
    global scenario

    scenario.trigger_switch = mysql_manager.execute_query("select switch from triggers")[1:][0][0]
    
    times = mysql_manager.execute_query("select min(time_in), max(time_out) from packetrecords")[1:][0]
    scenario.min_time = times[0]
    scenario.max_time = times[1]

    scenario.tor_switches = set( [x[0] for x in mysql_manager.execute_query("select switch from torswitches")[1:] ] )
    scenario.all_switches = set( [x[0] for x in mysql_manager.execute_query("select distinct switch from packetrecords")[1:] ] )
    scenario.all_flows = set( [x[0] for x in mysql_manager.execute_query("select distinct source_ip from packetrecords")[1:] ] )

    # Populate data on flows
    for flow in scenario.all_flows:
        scenario.flow_arr[flow] = Flow(identifier=flow)
        scenario.flow_arr[flow].populateSwitchList(mysql_manager)
        scenario.flow_arr[flow].populateRatios(mysql_manager)

    # Populate data on switches
    for switch in scenario.all_switches:
        scenario.switch_arr[switch] = Switch(switch)
        scenario.switch_arr[switch].populateFlowList(mysql_manager)
        scenario.switch_arr[switch].populateRatios(mysql_manager)



if __name__ == "__main__":

    generateScenarioData()
    generateTopologyGraph()
    getSwitchToLevelMapping()
    getIPAddressMapping()
    getTopologyPositions()

    app.run(debug=True)
