import mysql.connector
import requests
from flask import Flask, g, redirect, render_template, session, url_for, request
from flask_bootstrap import Bootstrap
from mysql.connector import Error
import os

from lib.core import (Dashboard, Dashboard_Properties, MySQL_Manager, Panel,
                      QueryBuilder, Target, Time, Grid_Position, Switch, Flow)
from lib.forms import PacketSearchForm, SampleForm, SimpleButton

from collections import deque
from lib.vars import DATABASE, HOST, URL, ANNOTATIONS_URL, DATASOURCE_URL, API_KEY, YEAR_SEC, UNIX_TIME_START_YEAR, headers

app = Flask(__name__)
app.config['SECRET_KEY'] = "Gangadhar hi Shaktimaan hai"

bootstrap = Bootstrap(app)

switchArr = {}
flowArr = {}
G = {}
levels = {}

mysql_manager = MySQL_Manager(database=DATABASE)
trigger_switch = mysql_manager.execute_query("select switch from triggers")[1:][0][0]
times = mysql_manager.execute_query("select min(time_in), max(time_out) from packetrecords")[1:][0]
min_time = times[0]
max_time = times[1]
tor_switches = set( [x[0] for x in mysql_manager.execute_query("select switch from torswitches")[1:] ] )
all_switches = set( [x[0] for x in mysql_manager.execute_query("select distinct switch from packetrecords")[1:] ] )
all_flows = set( [x[0] for x in mysql_manager.execute_query("select distinct source_ip from packetrecords")[1:] ] )

for flow in all_flows:
    flowArr[flow] = Flow(identifier=flow)
    flowArr[flow].populate_switch_list(mysql_manager)
    flowArr[flow].populate_ratios(mysql_manager)

for switch in all_switches:
    switchArr[switch] = Switch(switch)
    switchArr[switch].populate_flow_list(mysql_manager)
    switchArr[switch].populate_ratios(mysql_manager)



#Global declarations

@app.errorhandler(404)
def page_not_found(e):
    return render_template('404.html'), 404


@app.errorhandler(500)
def internal_server_error(e):
    return render_template('500.html'), 500


@app.route('/')
def index():
    return render_template('index.html', switch=trigger_switch, duration=(max_time-min_time) / 10**9)

@app.route('/switches')
def switches():
    lvlToSwitch = {}
    for key in levels:
        if levels[key] in lvlToSwitch:
            lvlToSwitch[levels[key]].append(key)
        else:
            lvlToSwitch[levels[key]] = [key]

    other_switches = all_switches - tor_switches
    return render_template('switches.html', lvlToSwitch = lvlToSwitch, tor_switches=sorted(list(tor_switches)), other_switches=sorted(list(other_switches)) , trigger_switch = trigger_switch)

@app.route('/switches/<switch>', methods=['GET', 'POST'])
def displaySwitch(switch):
    form = SimpleButton()
    # print(Swich.flowList)

    if request.method == "GET":
        return render_template('switchinfo.html', switch=switchArr[switch], form=form, level = levels[switch])
    elif request.method == "POST":
        if form.validate_on_submit():
            panelList = []

            panelList.append(Panel(gridPos=Grid_Position(x=0,y=0), title="Default Panel: Relative ratios of packets for each flow at Switch " + switch, targets = [Target(rawSql=QueryBuilder(time_column = "time_stamp", value= 'ratio', metricList = ['switch', 'source_ip'],  table='ratios', isConditional=True, conditionalClauseList=['switch = \'' + str(switch) + '\'']).get_generic_query())], datasource=DATABASE))
            panelList.append(Panel(gridPos=Grid_Position(x=0,y=22),title="Default Panel: Link Utilization", targets = [Target(rawSql=QueryBuilder(value = 'link_utilization', metricList = ['switch', 'source_ip'], isConditional=True, conditionalClauseList=['switch = \'' + str(switch) + '\'']).get_generic_query())], datasource=DATABASE))
            panelList.append(Panel(gridPos=Grid_Position(x=12,y=11),title="Default Panel: Queue Depth", targets = [Target(rawSql=QueryBuilder(value = 'queue_depth', metricList = ['switch'], isConditional=True, conditionalClauseList=['switch = \'' + str(switch) + '\'']).get_generic_query())], datasource=DATABASE))
            panelList.append(Panel(gridPos=Grid_Position(x=12,y=0),title="Packet distribution at trigger switch", targets = [Target(rawSql=QueryBuilder(value = 'source_ip % 10', metricList = ['source_ip'], isConditional=True, conditionalClauseList=['switch = \'' + str(switch) + '\'']).get_generic_query())], datasource=DATABASE, points = True, lines = False))
            panelList.append(Panel(gridPos=Grid_Position(x=0,y=11), title="Default Panel: Relative ratios of packets for each flow at Switch " + switch, targets = [Target(rawSql=QueryBuilder(time_column = "time_stamp", value= 'ratio', metricList = ['switch', 'source_ip'],  table='ratios', isConditional=True, conditionalClauseList=['switch = \'' + str(switch) + '\'']).get_generic_query())], datasource=DATABASE, lines = False, bars = True, stack = True, percentage = True))
            panelList.append(Panel(gridPos=Grid_Position(x=12,y=22),title="Default Panel: Instantaneous Ingress Throughput at Switch " + switch, targets = [Target(rawSql=QueryBuilder(value = 'throughput', metricList = ['switch', 'source_ip'], table='throughput', isConditional=True, conditionalClauseList=['switch = \'' + str(switch) + '\'']).get_generic_query())], datasource=DATABASE))

            time_from_seconds = g.mysql_manager.execute_query('select min(time_in) from packetrecords where switch = ' + switch)[1][0]
            time_to_seconds = g.mysql_manager.execute_query('select max(time_out) from packetrecords where switch = ' + switch)[1][0]

            year_from = UNIX_TIME_START_YEAR + (time_from_seconds // YEAR_SEC)
            year_to = UNIX_TIME_START_YEAR + 1 + (time_to_seconds // YEAR_SEC)
            
            time_from = get_formatted_time(year_from)
            time_to = get_formatted_time(year_to)

            dashboard = Dashboard(properties=Dashboard_Properties(title="Switch " + switch ,time=Time(timeFrom=time_from, timeTo=time_to)), panels=panelList)
            
            payload = get_final_payload(dashboard)
            # print("\nPayload:\n" + payload)
            response = requests.request("POST", url=URL, headers=headers, data = payload)
            dashboardId = response.json()['uid']
            return render_template('switchinfo.html', switch=switchArr[switch], form = form, dashboardID=dashboardId, level = levels[switch])

@app.route('/topo')
def topo():
    result_set = g.mysql_manager.execute_query("select * from links")
    print(result_set)
    return render_template('topology.html')

@app.route('/query', methods=['GET', 'POST'])
def query():
    form = SampleForm()
    results = None
    if form.validate_on_submit():
        q = QueryBuilder(time_column='time_in', value = form.value.data, metricList=['switch']).get_generic_query() + " LIMIT 10000"
        print(q)
        results = g.mysql_manager.execute_query(q)
    return render_template('query.html', form=form, results=results)

@app.route('/packetwise', methods=['GET', 'POST'])
def packetwise():

    form = PacketSearchForm()
    form2 = SimpleButton()
    results = None
    
    if request.method == "GET":
        return render_template('packetwise.html', form=form, form2=form2, results=results)
    
    elif request.method == "POST":
        if form.validate_on_submit():
            print("Here")
            q = "select time_in, switch from packetrecords where hash = " + form.hash.data + " order by time_in"
            results = g.mysql_manager.execute_query(q)
            results = results[1:]
            session['hash'] = form.hash.data
            session['results'] = results
            return render_template('packetwise.html', form=form, form2=form2, results=results)
        elif form2.validate_on_submit():
            print("here2")
            hash = form.hash.data
            
            time_from_seconds = g.mysql_manager.execute_query('select min(time_in) from packetrecords where hash = ' + session.get('hash'))[1][0]
            time_to_seconds = g.mysql_manager.execute_query('select max(time_out) from packetrecords where hash = ' + session.get('hash'))[1][0]

            year_from = UNIX_TIME_START_YEAR + (time_from_seconds // YEAR_SEC)
            year_to = UNIX_TIME_START_YEAR + 1 + (time_to_seconds // YEAR_SEC)
            
            time_from = get_formatted_time(year_from)
            time_to = get_formatted_time(year_to)

            q = "select time_in as \'time\', concat(switch) as metric, time_queue from packetrecords where hash = " + session.get('hash') + " order by time_in"

            panelList = []
            panelList.append(Panel(title="Packet " + session.get('hash'), targets = [Target(rawSql=q)], datasource=DATABASE))

            dashboard = Dashboard(properties=Dashboard_Properties(title="Packet " + hash ,time=Time(timeFrom=time_from, timeTo=time_to)), panels=panelList)
            payload = get_final_payload(dashboard)
            # print("\nPayload:\n" + payload)
            response = requests.request("POST", url=URL, headers=headers, data = payload)

            dashboardId = response.json()['uid']
    
            return render_template('packetwise.html', form=PacketSearchForm(), form2=form2, results = session.get('results'), dashboardId=dashboardId)
        else:
            print("Here3")
    

@app.route('/flows/<flow>')
def displayFlow(flow):
    # print(flow)
    lvlToSwitch = {}
    for switch in flowArr[int(flow)].switchList:
        if levels[switch] in lvlToSwitch:
            lvlToSwitch[levels[switch]].append(switch)
        else:
            lvlToSwitch[levels[switch]] = [switch]
    return render_template('flowinfo.html', flo = flowArr[int(flow)], lvlToSwitch=lvlToSwitch)

@app.route('/flows')
def flows():
    return render_template('flows.html', flows = flowArr, switches = switchArr, trigger_switch=trigger_switch)

@app.route('/grafana')
def grafana():
    return render_template('grafana.html')

@app.before_request
def before_request():
  g.mysql_manager = MySQL_Manager(database=DATABASE)
  print("Opened")

@app.teardown_request
def teardown_request(exception):
  db = getattr(g, 'db', None)
  if db is not None:
    db.close()

# @app.before_first_request
# def initialize():
#     mysql_manager = MySQL_Manager(database=DATABASE)
#     g.trigger_switch = mysql_manager.execute_query("select switch from triggers")[1:][0][0]
#     g.times = mysql_manager.execute_query("select min(time_in), max(time_out) from packetrecords")[1:][0]
#     g.min_time = g.times[0]
#     g.max_time = g.times[1]

def get_final_payload(dashboard):
    payload = "{ \"dashboard\": {" + dashboard.get_json_string() + "}, \"overwrite\": true}"
    return payload

def get_formatted_time(year):
    return "{}-{}-{}".format(year, "01", "01")

def generateGraph():
    links = mysql_manager.execute_query("select * from links")[1:]
    for link in links:
        u, v, cap = link[0], link[1], link[2]
        if u in G:
            G[u].append([v, cap])
        else:
            G[u] = []
            G[u].append([v, cap])

def getLevels():
    for tor in tor_switches:
        visited = []
        bfs(visited, deque([[tor, 1]]))
        # print(levels)
        
def bfs(visited, queue):
    if len(queue) == 0:
        return
    
    node = queue.popleft()
    if node[0] in levels:
        levels[node[0]] = min(levels[node[0]], node[1])
    else:
        levels[node[0]] = node[1]
    for adj in G[node[0]]:
        if adj[0] not in visited:
            visited.append(adj[0])
            queue.append([adj[0], node[1] + 1])
    
    bfs(visited, queue)

if __name__ == "__main__":

    generateGraph()
    getLevels()
    os.system('say "READY"')
    app.run(debug=True)
