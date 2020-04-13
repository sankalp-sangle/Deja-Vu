import mysql.connector
import requests
from flask import Flask, g, redirect, render_template, session, url_for
from flask_bootstrap import Bootstrap
from mysql.connector import Error

from lib.core import (Dashboard, Dashboard_Properties, MySQL_Manager, Panel,
                      QueryBuilder, Target, Time, Grid_Position, Switch, Flow)
from lib.forms import PacketSearchForm, SampleForm, SimpleButton

from lib.vars import DATABASE, HOST, URL, ANNOTATIONS_URL, DATASOURCE_URL, API_KEY, YEAR_SEC, UNIX_TIME_START_YEAR, headers

app = Flask(__name__)
app.config['SECRET_KEY'] = "Gangadhar hi Shaktimaan hai"

bootstrap = Bootstrap(app)

@app.errorhandler(404)
def page_not_found(e):
    return render_template('404.html'), 404


@app.errorhandler(500)
def internal_server_error(e):
    return render_template('500.html'), 500


@app.route('/')
def index():
    trigger_switch = g.mysql_manager.execute_query("select switch from triggers")[1:][0][0]
    times = g.mysql_manager.execute_query("select min(time_in), max(time_out) from packetrecords")[1:][0]
    min_time = times[0]
    max_time = times[1]
    return render_template('index.html', switch=trigger_switch, duration=(max_time-min_time) / 10**9)


@app.route('/user/<name>')
def user(name):
    return render_template('user.html', name=name)

@app.route('/display')
def display():
    result_set = g.mysql_manager.execute_query("select distinct switch from packetrecords")
    switches = [row[0] for row in result_set[1:]]
    return render_template('switches.html', switches=switches)

@app.route('/displaySwitch/<switch>', methods=['GET', 'POST'])
def displaySwitch(switch):
    Swich = Switch(switch)
    Swich.populate_flow_list(g.mysql_manager)
    Swich.populate_ratios(g.mysql_manager)
    # print(Swich.flowList)

    
    
    return render_template('switchinfo.html', switch=Swich)

    

@app.route('/switchPush/<switch>')
def switchPush(switch):
    panelList = []

    panelList.append(Panel(gridPos=Grid_Position(x=0,y=0), title="Default Panel: Relative ratios of packets for each flow at Switch " + switch, targets = [Target(rawSql=QueryBuilder(time_column = "time_stamp", value= 'ratio', metricList = ['switch', 'source_ip'],  table='ratios', isConditional=True, conditionalClauseList=['switch = \'' + str(switch) + '\'']).get_generic_query())], datasource=DATABASE))
    panelList.append(Panel(gridPos=Grid_Position(x=0,y=22),title="Default Panel: Link Utilization", targets = [Target(rawSql=QueryBuilder(value = 'link_utilization', metricList = ['switch', 'source_ip'], isConditional=True, conditionalClauseList=['switch = \'' + str(switch) + '\'']).get_generic_query())], datasource=DATABASE))
    panelList.append(Panel(gridPos=Grid_Position(x=12,y=11),title="Default Panel: Queue Depth", targets = [Target(rawSql=QueryBuilder(value = 'queue_depth', metricList = ['switch'], isConditional=True, conditionalClauseList=['switch = \'' + str(switch) + '\'']).get_generic_query())], datasource=DATABASE))
    panelList.append(Panel(gridPos=Grid_Position(x=12,y=0),title="Packet distribution at trigger switch", targets = [Target(rawSql=QueryBuilder(value = 'source_ip % 10', metricList = ['source_ip'], isConditional=True, conditionalClauseList=['switch = \'' + str(switch) + '\'']).get_generic_query())], datasource=DATABASE, points = True, lines = False))
    panelList.append(Panel(gridPos=Grid_Position(x=0,y=11), title="Default Panel: Relative ratios of packets for each flow at Switch " + switch, targets = [Target(rawSql=QueryBuilder(time_column = "time_stamp", value= 'ratio', metricList = ['switch', 'source_ip'],  table='ratios', isConditional=True, conditionalClauseList=['switch = \'' + str(switch) + '\'']).get_generic_query())], datasource=DATABASE, lines = False, bars = True, stack = True, percentage = True))

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
    return render_template('switchPush.html', response=str(response.json()), dashboardID=dashboardId)

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
    if form.validate_on_submit():
        q = "select time_in, switch from packetrecords where hash = " + form.hash.data + " order by time_in"
        results = g.mysql_manager.execute_query(q)
        results = results[1:]
        session['hash'] = form.hash.data
    elif form2.validate_on_submit():
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
        return render_template('result.html', response=str(response.json()))
    return render_template('packetwise.html', form=form, form2=form2, results=results)

@app.route('/flow/<flow>')
def flow(flow):
    Flowe = Flow(identifier=flow)
    Flowe.populate_switch_list(g.mysql_manager)
    Flowe.populate_ratios(g.mysql_manager)

    return render_template('flowinfo.html', flo = Flowe)

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

def get_final_payload(dashboard):
    payload = "{ \"dashboard\": {" + dashboard.get_json_string() + "}, \"overwrite\": true}"
    return payload

def get_formatted_time(year):
    return "{}-{}-{}".format(year, "01", "01")

if __name__ == "__main__":
    app.run(debug=True)
