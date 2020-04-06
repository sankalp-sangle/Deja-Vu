import mysql.connector
import requests
from flask import Flask, g, redirect, render_template, session, url_for
from flask_bootstrap import Bootstrap
from mysql.connector import Error

from lib.core import (Dashboard, Dashboard_Properties, MySQL_Manager, Panel,
                      QueryBuilder, Target, Time)
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
    return render_template('index.html')


@app.route('/user/<name>')
def user(name):
    return render_template('user.html', name=name)

@app.route('/display')
def display():
    result_set = g.mysql_manager.execute_query("select distinct switch from packetrecords")
    switches = [row[0] for row in result_set[1:]]
    return render_template('switches.html', switches=switches)

@app.route('/displaySwitch/<switch>')
def displaySwitch(switch):
    result_set = g.mysql_manager.execute_query("select * from packetrecords where switch='" + switch + "'")
    return render_template('switchinfo.html', records=result_set[1:])

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
