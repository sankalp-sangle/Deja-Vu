import json
import os
from collections import deque

import mysql.connector
import requests
from flask import (Flask, g, redirect, render_template, request, session,
                   url_for)
from flask_bootstrap import Bootstrap
from mysql.connector import Error

from lib.core import (Dashboard, Dashboard_Properties, Flow, Grid_Position,
                      MySQL_Manager, Panel, QueryBuilder, Switch, Target, Time, Scenario)
from lib.forms import PacketSearchForm, QueryForm, RandomQuery, SimpleButton
from lib.vars import (ANNOTATIONS_URL, API_KEY, COLORS, DATABASE,
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
    return render_template('404.html'), 404


@app.errorhandler(500)
def internal_server_error(e):
    return render_template('500.html'), 500


@app.route('/')
def index():
    return render_template('index.html', switch=scenario.trigger_switch, duration=(scenario.max_time-scenario.min_time) / 10**9)

@app.route('/switches')
def switches():
    lvlToSwitch = {}
    for key in scenario.switchToLevelMapping:
        if scenario.switchToLevelMapping[key] in lvlToSwitch:
            lvlToSwitch[scenario.switchToLevelMapping[key]].append(key)
        else:
            lvlToSwitch[scenario.switchToLevelMapping[key]] = [key]

    other_switches = scenario.all_switches - scenario.tor_switches
    return render_template('switches.html', lvlToSwitch = lvlToSwitch, tor_switches=sorted(list(scenario.tor_switches)), other_switches=sorted(list(other_switches)) , trigger_switch = scenario.trigger_switch)

@app.route('/switches/<switch>', methods=['GET', 'POST'])
def displaySwitch(switch):
    global scenario
    form = SimpleButton()

    points = []
    for flow in scenario.switchArr[switch].flowList:
        entry = {}
        entry['SourceIP'] = scenario.mapIp[flow]
        entry['Ratio'] = scenario.switchArr[switch].ratios[flow]
        points.append(entry)

    if request.method == "GET":
        return render_template('switchinfo.html', points=points, mapIp=scenario.mapIp, switch=scenario.switchArr[switch], form=form, level = scenario.switchToLevelMapping[switch])
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
            
            time_from = get_formatted_time(year_from)
            time_to = get_formatted_time(year_to)

            dashboard = Dashboard(properties=Dashboard_Properties(title="Switch " + switch ,time=Time(timeFrom=time_from, timeTo=time_to)), panels=panelList)
            
            payload = get_final_payload(dashboard)
            response = requests.request("POST", url=URL, headers=headers, data = payload)
            dashboardId = response.json()['uid']
            return render_template('switchinfo.html', points = points, mapIp=scenario.mapIp, switch=scenario.switchArr[switch], form = form, dashboardID=dashboardId, level = scenario.switchToLevelMapping[switch])

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
            
            time_from = get_formatted_time(year_from)
            time_to = get_formatted_time(year_to)

            q = "select time_in as \'time\', concat(switch) as metric, time_queue from packetrecords where hash = " + session.get('hash') + " order by time_in"

            panelList = []
            panelList.append(Panel(title="Packet " + session.get('hash'), targets = [Target(rawSql=q)], datasource=DATABASE))

            dashboard = Dashboard(properties=Dashboard_Properties(title="Packet " + hash ,time=Time(timeFrom=time_from, timeTo=time_to)), panels=panelList)
            payload = get_final_payload(dashboard)

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
    for switch in scenario.flowArr[int(flow)].switchList:
        if scenario.switchToLevelMapping[switch] in lvlToSwitch:
            lvlToSwitch[scenario.switchToLevelMapping[switch]].append(switch)
        else:
            lvlToSwitch[scenario.switchToLevelMapping[switch]] = [switch]
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

    return render_template('flowinfo.html', mapIp=scenario.mapIp, flo = scenario.flowArr[int(flow)], lvlToSwitch=lvlToSwitch, nodelist=scenario.nodelist, linklist=scenario.linklist, times=times, maxLim=maxLim, minLim = minLim, minLim2 = minLim2, maxLim2=maxLim2, choices = choices)

@app.route('/flows')
def flows():
    return render_template('flows.html', mapIp=scenario.mapIp, flows = scenario.flowArr, switches = scenario.switchArr, trigger_switch=scenario.trigger_switch)

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
        res = g.mysql_manager.execute_query("select switch, min(time_in) from packetrecords where source_ip=\'"+ str(scenario.flowArr[flow].identifier) + "\' group by switch")[1:]
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


    return render_template('allflows.html', mapIp = scenario.mapIp, flowIPS=requestedFlows, noOfFlows=len(requestedFlows), noOfFlowsJS = {"val":len(requestedFlows)}, nodelist=scenario.nodelist, linklist=scenario.linklist, times=times, maxLim=maxLim, minLim = minLim, minLim2 = minLim2, maxLim2=maxLim2)

@app.before_request
def before_request():
  g.mysql_manager = MySQL_Manager(database=DATABASE)
  print("Opened")

@app.teardown_request
def teardown_request(exception):
  db = getattr(g, 'db', None)
  if db is not None:
    db.close()

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

    return render_template('general.html', triggerNode = {"id":triggerNode}, triggerTime = {"val":triggerTime}, levels = scenario.switchToLevelMapping, maxD = {"val":(maxDepth * 80) // 1500}, throughputlimits=throughputlimits, nodelist=scenario.nodelist, throughput = throughput, linklist=scenario.linklist, limits = limits, datas=datas, maxLim=maxLim, minLim = minLim, minLim2 = minLim2, maxLim2=maxLim2)

@app.route('/test/<from_switch>/<to_switch>')
def test(from_switch, to_switch):
    res = g.mysql_manager.execute_query('select time_in, throughput from egressthroughput where from_switch = \'' + from_switch + '\' and to_switch = \'' + to_switch + '\' order by time_out')[1:]
    datas = []
    entry = {}
    inner = {}

    for i,row in enumerate(res):
        datas.append({'date':row[0], 'value':str(row[1])})    

    return render_template('test.html', data=datas)

def get_final_payload(dashboard):
    payload = "{ \"dashboard\": {" + dashboard.get_json_string() + "}, \"overwrite\": true}"
    return payload

def get_formatted_time(year):
    return "{}-{}-{}".format(year, "01", "01")

def generate_topology_graph():
    global scenario

    links = mysql_manager.execute_query("select * from links")[1:]
    for link in links:
        u, v, cap = link[0], link[1], link[2]
        if u in scenario.topologyGraph:
            scenario.topologyGraph[u].append([v, cap])
        else:
            scenario.topologyGraph[u] = []
            scenario.topologyGraph[u].append([v, cap])

def get_switch_to_level_mapping():
    global scenario
    for tor in scenario.tor_switches:
        visited = []
        bfs(visited, deque([[tor, 1]]))
        
def bfs(visited, queue):
    global scenario
    if len(queue) == 0:
        return
    
    node = queue.popleft()
    if node[0] in scenario.switchToLevelMapping:
        scenario.switchToLevelMapping[node[0]] = min(scenario.switchToLevelMapping[node[0]], node[1])
    else:
        scenario.switchToLevelMapping[node[0]] = node[1]
    for adj in scenario.topologyGraph[node[0]]:
        if adj[0] not in visited:
            visited.append(adj[0])
            queue.append([adj[0], node[1] + 1])
    
    bfs(visited, queue)

def get_topology_positions():
    global scenario
    BEGINX = 20
    BEGINY = 70
    nodesAddedToLevel = {}
    for node in scenario.switchToLevelMapping:
        nodesAddedToLevel[scenario.switchToLevelMapping[node]] = []

    for node in scenario.topologyGraph:
        nodeEntry = {}
        nodeEntry["id"] = node
        nodeEntry["group"] = "switch"
        nodeEntry["label"] = node
        nodeEntry["level"] = scenario.switchToLevelMapping[node]
        nodesAddedToLevel[scenario.switchToLevelMapping[node]].append(node)
        nodeEntry["x"] = BEGINX + 200 * len(nodesAddedToLevel[scenario.switchToLevelMapping[node]])
        nodeEntry["y"] = BEGINY + (3-scenario.switchToLevelMapping[node]) * 200
        scenario.nodelist.append(nodeEntry)
    
    links = mysql_manager.execute_query("select * from links")[1:]

    for link in links:
        linkEntry = {}
        linkEntry["source"] = link[0]
        linkEntry["target"] = link[1]
        linkEntry["strength"] = 0.7
        scenario.linklist.append(linkEntry)

def Int2IP(ipnum):
    o1 = int(ipnum / pow(2,24)) % 256
    o2 = int(ipnum / pow(2,16)) % 256
    o3 = int(ipnum / pow(2,8)) % 256
    o4 = int(ipnum) % 256
    return '%(o1)s.%(o2)s.%(o3)s.%(o4)s' % locals()

def IP2Int(ip):
    o = list(map(int, ip.split('.')))
    res = (16777216 * o[0]) + (65536 * o[1]) + (256 * o[2]) + o[3]
    return res

def get_ip_addresses():
    global scenario
    result = mysql_manager.execute_query('select distinct source_ip from packetrecords')

    for row in result[1:]:
        for ip in row:
            scenario.mapIp[ip] = Int2IP(ip)

def getPanels(mysql_manager, switch):
    global scenario
    panelList = []
    colorMap = {}

    indexOfAvailableColour = 0
    
    q1 = QueryBuilder(time_column = "time_stamp * " + str(MAX_LEGAL_UNIX_TIMESTAMP) + " / " + str(scenario.max_time), value= 'ratio', metricList = ['switch', 'source_ip'],  table='ratios', isConditional=True, conditionalClauseList=['switch = \'' + str(switch) + '\'']).get_generic_query()
    aliasColors, indexOfAvailableColour, colorMap = getAliasColors(mysql_manager, q1, colorMap, indexOfAvailableColour)
    panelList.append(Panel(gridPos=Grid_Position(x=0,y=0), title="Default Panel: Relative ratios of packets for each flow at Switch " + switch, targets = [Target(rawSql=q1)], datasource=DATABASE, aliasColors=aliasColors))
    
    q2 = QueryBuilder(time_column = "time_stamp * " + str(MAX_LEGAL_UNIX_TIMESTAMP) + " / " + str(scenario.max_time), value= 'ratio*total_pkts', metricList = ['switch', 'source_ip'],  table='ratios', isConditional=True, conditionalClauseList=['switch = \'' + str(switch) + '\'']).get_generic_query()
    aliasColors, indexOfAvailableColour, colorMap = getAliasColors(mysql_manager, q2, colorMap, indexOfAvailableColour)
    panelList.append(Panel(gridPos=Grid_Position(x=0,y=11), title="Default Panel: Relative ratios of packets for each flow at Switch " + switch, targets = [Target(rawSql=q2)], datasource=DATABASE, lines = False, bars = True, stack = True, percentage = False, aliasColors=aliasColors))

    q3 = QueryBuilder(time_column = "time_in * " + str(MAX_LEGAL_UNIX_TIMESTAMP) + " / " + str(scenario.max_time), value = 'throughput', metricList = ['from_switch','to_switch'], table='egressthroughput', isConditional=True, conditionalClauseList=['from_switch = \'' + str(switch) + '\'']).get_generic_query()
    aliasColors, indexOfAvailableColour, colorMap = getAliasColors(mysql_manager, q3, colorMap, indexOfAvailableColour)
    panelList.append(Panel(gridPos=Grid_Position(x=12,y=22),title="Default Panel: Instantaneous Egress Throughput at Switch " + switch, targets = [Target(rawSql=q3)], datasource=DATABASE, aliasColors=aliasColors))

    q4 = QueryBuilder(time_column = "time_in * " + str(MAX_LEGAL_UNIX_TIMESTAMP) + " / " + str(scenario.max_time), value = 'link_utilization', metricList = ['switch', 'source_ip'], isConditional=True, conditionalClauseList=['switch = \'' + str(switch) + '\'']).get_generic_query()
    aliasColors, indexOfAvailableColour, colorMap = getAliasColors(mysql_manager, q4, colorMap, indexOfAvailableColour)
    panelList.append(Panel(gridPos=Grid_Position(x=0,y=33),title="Default Panel: Link Utilization", targets = [Target(rawSql=q4)], datasource=DATABASE, aliasColors=aliasColors))

    q5 = QueryBuilder(time_column = "time_in * " + str(MAX_LEGAL_UNIX_TIMESTAMP) + " / " + str(scenario.max_time), value = 'source_ip % 10', metricList = ['switch', 'source_ip'], isConditional=True, conditionalClauseList=['switch = \'' + str(switch) + '\'']).get_generic_query()
    aliasColors, indexOfAvailableColour, colorMap = getAliasColors(mysql_manager, q5, colorMap, indexOfAvailableColour)
    panelList.append(Panel(gridPos=Grid_Position(x=12,y=0),title="Packet distribution at trigger switch", targets = [Target(rawSql=q5)], datasource=DATABASE, points = True, lines = False, aliasColors=aliasColors))
    
    q6 = QueryBuilder(time_column = "time_in * " + str(MAX_LEGAL_UNIX_TIMESTAMP) + " / " + str(scenario.max_time), value = 'queue_depth', metricList = ['switch'], isConditional=True, conditionalClauseList=['switch = \'' + str(switch) + '\'']).get_generic_query()
    aliasColors, indexOfAvailableColour, colorMap = getAliasColors(mysql_manager, q6, colorMap, indexOfAvailableColour)
    panelList.append(Panel(gridPos=Grid_Position(x=12,y=11),title="Default Panel: Queue Depth", targets = [Target(rawSql=q6)], datasource=DATABASE, aliasColors=aliasColors))

    q7 = QueryBuilder(time_column = "time_in * " + str(MAX_LEGAL_UNIX_TIMESTAMP) + " / " + str(scenario.max_time), value = 'throughput', metricList = ['switch', 'source_ip'], table='throughput', isConditional=True, conditionalClauseList=['switch = \'' + str(switch) + '\'']).get_generic_query()
    aliasColors, indexOfAvailableColour, colorMap = getAliasColors(mysql_manager, q7, colorMap, indexOfAvailableColour)
    panelList.append(Panel(gridPos=Grid_Position(x=12,y=22),title="Default Panel: Instantaneous Ingress Throughput at Switch " + switch, targets = [Target(rawSql=q7)], datasource=DATABASE, aliasColors=aliasColors))

    return panelList

def getAliasColors(mysql_manager, q, colorMap, indexOfAvailableColour):
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

def generate_scenario_data():
    global scenario

    scenario.trigger_switch = mysql_manager.execute_query("select switch from triggers")[1:][0][0]
    times = mysql_manager.execute_query("select min(time_in), max(time_out) from packetrecords")[1:][0]
    scenario.min_time = times[0]
    scenario.max_time = times[1]
    scenario.tor_switches = set( [x[0] for x in mysql_manager.execute_query("select switch from torswitches")[1:] ] )
    scenario.all_switches = set( [x[0] for x in mysql_manager.execute_query("select distinct switch from packetrecords")[1:] ] )
    scenario.all_flows = set( [x[0] for x in mysql_manager.execute_query("select distinct source_ip from packetrecords")[1:] ] )

    for flow in scenario.all_flows:
        scenario.flowArr[flow] = Flow(identifier=flow)
        scenario.flowArr[flow].populate_switch_list(mysql_manager)
        scenario.flowArr[flow].populate_ratios(mysql_manager)

    for switch in scenario.all_switches:
        scenario.switchArr[switch] = Switch(switch)
        scenario.switchArr[switch].populate_flow_list(mysql_manager)
        scenario.switchArr[switch].populate_ratios(mysql_manager)



if __name__ == "__main__":

    generate_scenario_data()
    generate_topology_graph()
    get_switch_to_level_mapping()
    get_ip_addresses()
    get_topology_positions()

    app.run(debug=True)
