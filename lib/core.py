import mysql.connector
from mysql.connector import Error

from lib.config import MYSQL_USER, MYSQL_PASSWORD

'''
Note: For classes prefixed with "Grafana", each class represents a
component in the JSON model for a Grafana Dashboard. Dashboards in
Grafana are specified using a JSON string sent over HTTP API.
Every such class takes specifications for its JSON properties 
through parameters and generates the JSON string representation 
using the get_json_string method.


For more information on the JSON model for Grafana dashboards, refer:
https://grafana.com/docs/grafana/latest/reference/dashboard/
'''


class Scenario:
    '''
    A class to store information regarding the trace/scenario loaded into
    MySQL for visualization.
    Properties:
    switch_arr     : Dictionary, key -> switch ID : str, value -> Object of Switch class
    flow_arr       : Dictionary, key -> flow source ip : int, value -> Object of Flow class
    topology_graph : Dictionary, key -> switch ID : str, value -> neibhbouring switch IDs : List
    switch_to_level_mapping : Dictionary, key -> switch ID : str, value -> level of the switch : int
    map_ip : Dictionary, key -> flow source ip : int, value -> human readable source ip : str
    nodelist : list of dictionarys, each dictionary representing the following properties of a node
    id, group, label, level, x (coordinate in SVG element in frontend), y (coordinate in SVG element in frontend)
    List is needed for plotting topology in frontend (requirement of d3.js frontend library)
    linklist : list of dictionarys, each dictionary representing the following proerties of a link between
    two nodes 
    source, target, strength
    List is needed for plotting topology in frontend (requirement of d3.js frontend library)
    trigger_switch : switch ID of switch that generated the trigger : str
    min_time : earliest time of recording of any p record across all switches : int
    max_time : latest time of recording of any p record across all switches : int
    tor_switches : list of switch IDs representing all Top of Rack switches : list
    all_switches : list of switch IDs of all switches : list
    all_flows : list of source IPs of all flows : list
    '''
    
    def __init__(self):
        self.switch_arr = {}
        self.flow_arr = {}
        self.topology_graph = {}
        self.switch_to_level_mapping = {}
        self.map_ip = {}
        self.nodelist = []
        self.linklist = []
        self.trigger_switch = None
        self.min_time = None
        self.max_time = None
        self.tor_switches = []
        self.all_switches = []
        self.all_flows = []

class Grafana_Datasource:
    '''
    Refer to comment at top of file for purpose and usage
    '''

    DEFAULT_NAME = "No name given"
    DEFAULT_TYPE = "mysql"
    DEFAULT_DATABASE = "No name given to database"
    DEFAULT_USER = MYSQL_USER
    DEFAULT_PASSWORD = MYSQL_PASSWORD
        
    def __init__(self, name = None, database_type = None, database = None, user = None, password = None):
        if name is None:
            name = Grafana_Datasource.DEFAULT_NAME
        if database_type is None:
            database_type = Grafana_Datasource.DEFAULT_TYPE
        if database is None:
            database = Grafana_Datasource.DEFAULT_DATABASE
        if user is None:
            user = Grafana_Datasource.DEFAULT_USER
        if password is None:
            password = Grafana_Datasource.DEFAULT_PASSWORD

        self.name = name
        self.database_type = database_type
        self.database = database
        self.user = user
        self.password = password

    def get_json_string(self):
        return "\"name\": \"{}\", \"type\": \"{}\", \"url\":\"\", \"access\":\"proxy\", \"database\":\"{}\",\"user\":\"{}\", \"password\": \"{}\", \"basicAuth\":false".format(self.name, self.database_type, self.database, self.user, self.password)
        

        
class Grafana_Dashboard:
    '''
    Refer to comment at top of file for purpose and usage
    '''

    def __init__(self, panels = None, properties = None):
        if properties is None:
            properties = Grafana_Dashboard_Properties()
        if panels is None:
            panels = [Grafana_Panel(title="Sample Panel")]

        self.properties          = properties
        self.panels              = panels
        self.json_representation = ""

    def get_json_string(self):
        panelJSON = self.get_collective_panels_json()
        return "{}, \"panels\":[{}]".format(self.properties.get_json_string(), panelJSON)

    def get_collective_panels_json(self):
        if self.panels is []:
            return ""
            
        panelJSON = ""
        for panel in self.panels:
            panelJSON += "{" + panel.get_json_string() + "},"

        #Remove trailing comma
        panelJSON = panelJSON[:-1]
        
        return panelJSON

class Grafana_Dashboard_Properties:
    '''
    Refer to comment at top of file for purpose and usage
    '''

    ANNOTATION_JSON = '''\"annotations\": {
        \"list\": [
        {
            \"builtIn\": 1,
            \"datasource\": \"-- Grafana --\",
            \"enable\": true,
            \"hide\": true,
            \"iconColor\": "rgba(255, 255, 0, 1)\",
            \"type\": "dashboard\"
        }
        ]
    }'''
    def __init__(self, id = None, uid = None, title = None, timezone = None, schemaVersion = None, version = None, time = None):
        if id is None:
            id = "null"
        if uid is None:
            uid = "null"
        if title is None:
            title = "No title assigned to this dashboard"
        if timezone is None:
            timezone = "browser"
        if schemaVersion is None:
            schemaVersion = "21"
        if version is None:
            version = "0"
        if time is None:
            time = Grafana_Time()
            
        self.id = id
        self.uid = uid
        self.title = title
        self.timezone = timezone
        self.schemaVersion = schemaVersion
        self.version = version
        self.json_representation = ""
        self.time = time

    def get_json_string(self):
        return "{}, \"id\": {},\"uid\": {},\"title\": \"{}\",\"timezone\": \"{}\",\"schemaVersion\": {},\"version\": {},\"time\":{}".format(Grafana_Dashboard_Properties.ANNOTATION_JSON, self.id, self.uid, self.title, self.timezone, self.schemaVersion, self.version, "{" + self.time.get_json_string() + "}")

class Grafana_Grid_Position:
    '''
    Refer to comment at top of file for purpose and usage
    '''

    
    DEFAULT_HEIGHT = 9
    DEFAULT_WIDTH = 12

    def __init__(self, x = None, y = None, height = None, width = None):
        if x is None:
            x = 0
        if y is None:
            y = 0
        if height is None:
            height = Grafana_Grid_Position.DEFAULT_HEIGHT
        if width is None:
            width = Grafana_Grid_Position.DEFAULT_WIDTH

        self.x = x
        self.y = y
        self.height = height
        self.width = width

    def get_json_string(self):
        return "\"h\": {}, \"w\": {}, \"x\": {}, \"y\": {}".format(self.height, self.width, self.x, self.y) 

class Grafana_Panel:
    '''
    Refer to comment at top of file for purpose and usage
    '''


    GLOBAL_ID = 1
    DEFAULT_DATASOURCE = "MySQL"
    DEFAULT_TITLE = "This is a sample panel title!"
    DEFAULT_PANEL_TYPE = "graph"

    def __init__(self, datasource = None, id = None, title = None, panelType = None, gridPos = None, targets = None, xaxis = None, yaxes = None, lines = None, points = None, bars = None, stack = None, percentage = None, aliasColors = None):
        if datasource is None:
            datasource = Grafana_Panel.DEFAULT_DATASOURCE
        if id is None:
            id = str(Grafana_Panel.GLOBAL_ID)
            Grafana_Panel.GLOBAL_ID += 1
        if title is None:
            title = Grafana_Panel.DEFAULT_TITLE
        if panelType is None:
            panelType = Grafana_Panel.DEFAULT_PANEL_TYPE
        if gridPos is None:
            gridPos = Grafana_Grid_Position()
        if targets is None:
            targets = [Grafana_Target()]
        if xaxis is None:
            xaxis = Grafana_Xaxis()
        if yaxes is None:
            yaxes = Grafana_Yaxes()
        if lines is None:
            lines = True
        if points is None:
            points = False
        if bars is None:
            bars = False
        if stack is None:
            stack = False
        if percentage is None:
            percentage = False
        if aliasColors is None:
            aliasColors = ""

        self.datasource = datasource
        self.id = id
        self.title = title
        self.panelType = panelType
        self.gridPos = gridPos
        self.targets = targets
        self.xaxis = xaxis
        self.yaxes = yaxes
        self.lines = lines
        self.points = points
        self.bars = bars
        self.stack = stack
        self.percentage = percentage
        self.aliasColors = aliasColors

    def get_collective_targets_json(self):
        if self.targets is []:
            return ""
            
        targetJSON = ""
        for target in self.targets:
            targetJSON += "{" + target.get_json_string() + "},"

        #Remove trailing comma
        targetJSON = targetJSON[:-1]
        
        return targetJSON

    def get_json_string(self):
        targetJSON = self.get_collective_targets_json()
        return "\"datasource\": \"{}\",\"id\": {},\"title\": \"{}\",\"type\":\"{}\",\"gridPos\":{}, \"targets\": [{}], \"xaxis\": {}, \"yaxes\": [{}], \"lines\": {}, \"points\": {}, \"bars\": {}, \"stack\": {}, \"percentage\": {}, \"aliasColors\": {}".format(self.datasource, self.id, self.title, self.panelType, "{" + self.gridPos.get_json_string() + "}", targetJSON, "{" + self.xaxis.get_json_string() + "}", self.yaxes.get_json_string(), "true" if self.lines else "false", "true" if self.points else "false","true" if self.bars else "false","true" if self.stack else "false","true" if self.percentage else "false", "{" + self.aliasColors + "}")

class Grafana_Xaxis:
    '''
    Refer to comment at top of file for purpose and usage
    '''

    def __init__(self, showAxis = None):
        if showAxis is None:
            showAxis = False

        self.showAxis = showAxis

    def get_json_string(self):
        return "\"show\": {}".format("true" if self.showAxis else "false")

class Grafana_Yaxes:
    '''
    Refer to comment at top of file for purpose and usage
    '''

    DEFAULT_FORMAT = "short"

    def __init__(self, leftAxisLabel = None, rightAxisLabel = None, leftAxisFormat = None, rightAxisFormat = None):

        if leftAxisFormat is None:
            leftAxisFormat = Grafana_Yaxes.DEFAULT_FORMAT
        if rightAxisFormat is None:
            rightAxisFormat = Grafana_Yaxes.DEFAULT_FORMAT

        self.leftAxisLabel = leftAxisLabel
        self.rightAxisLabel = rightAxisLabel
        self.leftAxisFormat = leftAxisFormat
        self.rightAxisFormat = rightAxisFormat

    def get_json_string(self):
        return "{" + "\"label\": {}".format("null" if self.leftAxisLabel == None else "\"" + self.leftAxisLabel + "\"") + "," +  "\"format\":\"{}\"".format(self.leftAxisFormat) + ", \"show\":true},{" + "\"label\": {}".format("null" if self.rightAxisLabel == None else "\"" + self.rightAxisLabel + "\"") + "," +  "\"format\":\"{}\"".format(self.rightAxisFormat) + "," + " \"show\":true}"

class Grafana_Target:
    '''
    Refer to comment at top of file for purpose and usage
    '''


    DEFAULT_FORMAT = "time_series"
    DEFAULT_RAW_SQL = ""
    DEFAULT_REFID = "A"

    def __init__(self, format = None, rawQuery = None, rawSql = None, refId = None):
        if format is None:
            format = Grafana_Target.DEFAULT_FORMAT
        if rawQuery is None:
            rawQuery = True
        if rawSql is None:
            rawSql = Grafana_Target.DEFAULT_RAW_SQL
        if refId is None:
            refId = Grafana_Target.DEFAULT_REFID

        self.format = format
        self.rawQuery = rawQuery
        self.rawSql = rawSql
        self.refId = refId

    def get_json_string(self):
        return "\"format\": \"{}\",\"rawQuery\": {},\"rawSql\": \"{}\",\"refId\": \"{}\"".format(self.format, "true" if self.rawQuery else "false", self.rawSql, self.refId)

class Grafana_Time:
    '''
    Refer to comment at top of file for purpose and usage
    '''


    @staticmethod
    def convert_to_standard_format(timeFormat, requiresConversion):
            if requiresConversion:
                return "\"" + timeFormat[0:10] + "T00:00:00.000Z" + "\""
            return "\"" + timeFormat + "\""

    # Standard format : YYYY-MM-DD
    # Expected Grafana format : YYYY-MM-DDTHH:MM:SS.MSSZ

    def __init__(self, timeFrom = None, timeTo = None, fromRequiresConversion = True, toRequiresConversion = True):
        if timeFrom is None:
            timeFrom = "now"
            fromRequiresConversion = False

        if timeTo is None:
            timeTo = "now - 6h"
            toRequiresConversion = False

        self.timeFrom = timeFrom
        self.timeTo = timeTo
        self.fromRequiresConversion = fromRequiresConversion
        self.toRequiresConversion = toRequiresConversion

    def get_json_string(self):
        self.timeFrom = Grafana_Time.convert_to_standard_format(self.timeFrom, self.fromRequiresConversion)
        self.timeTo = Grafana_Time.convert_to_standard_format(self.timeTo, self.toRequiresConversion)
        return "\"from\": {},\"to\": {}".format(self.timeFrom, self.timeTo)

class MySQL_Manager:
    '''
    Purpose: A class to help communicate with the MySQL instance
    and execute queries.
    '''

    DEFAULT_HOST = 'localhost'
    DEFAULT_DATABASE = 'netplay'
    DEFAULT_USER = 'sankalp'
    DEFAULT_PASSWORD = 'sankalp'

    def __init__(self, connector = None, host = None, database = None, user = None, password = None):
        if host is None:
            host = MySQL_Manager.DEFAULT_HOST
        if database is None:
            database = MySQL_Manager.DEFAULT_DATABASE
        if user is None:
            user = MySQL_Manager.DEFAULT_USER
        if password is None:
            password = MySQL_Manager.DEFAULT_PASSWORD

        self.host = host
        self.database = database
        self.user = user
        self.password = password
        
        try:
            self.connector = mysql.connector.connect(host = self.host, database = self.database, user = self.user, password = self.password)
            if self.connector.is_connected():
                db_Info = self.connector.get_server_info()
                print("Connected to MySQL Server version ", db_Info)

        except Error as e:
            print("Error while connecting to MySQL", e)
    
    def execute_query(self, query):
        '''
        Parameters:
        query : str

        Executes query on MySQL instance and returns the result set
        as a collection of rows if there is a result returned.
        '''
        cursor = self.connector.cursor()
        try:
            cursor.execute(query)
        except Error as e:
            print("Failed to execute query", e)
            return []
        finally:
            # If result is returned, return the result set.
            # Else, commit the database (needed for update / delete)
            # queries.
            if cursor.with_rows:
                fields = [row[0] for row in cursor.description]
                resultset = cursor.fetchall()
                resultset.insert(0, fields)
                return resultset
        self.connector.commit()

class Switch:
    '''
    A class to model a switch.
    Properties:
    identifier : the ID of the switch : str
    flow_list : list of source ips of all flows going through this switch : list
    ratios : Dictionary, key -> source ip of flow : int, value : ratio of packets 
    of that flow passing through switch to total packets passing through switch : float
    '''
    def __init__(self, identifier = None):
        if identifier is None:
            identifier = 'No switch identifier'
        
        self.identifier = identifier
        self.flow_list = []
        self.ratios = {}

    def populateFlowList(self, my_sql_manager):
        '''
        Parameters: mysql_manager : Object of type MySQL_Manager class
        
        Purpose: Collects source ips of flows passing through switch and
        populates the flow_list attribute.
        '''
        if my_sql_manager is not None:
            result = my_sql_manager.execute_query('select distinct source_ip from packetrecords where switch = \'' + self.identifier + '\'')
            for row in result[1:]:
                self.flow_list.append(row[0])

    def populateRatios(self, mysql_manager):
        '''
        Parameters: mysql_manager : Object of type MySQL_Manager class
        
        Purpose: Calculates ratio for each source ip and populates
        ratios attribute.
        '''
        if mysql_manager is not None:
            result = mysql_manager.execute_query('select source_ip, count(hash) from packetrecords where switch =\'' + self.identifier + '\' group by source_ip')
            total_pkts = sum([row[1] for row in result[1:]])
            
            for row in result[1:]:
                self.ratios[row[0]] = row[1]/total_pkts

    def print_info(self, mapIp):
        '''
        Purpose: prints information about switch object
        '''

        print("Switch Identifier: S" + str(self.identifier))

        print("Flows passed:", end="")
        for flow in self.flow_list:
            print(mapIp[flow] if flow in mapIp else flow, end=" ")
        print("\nRatios:")
        for flow in self.ratios:
            print("Flow " + mapIp[flow] + ": " + str(self.ratios[flow]))
        print("\n")

class Flow:
    '''
    A class to model a flow.
    Properties:
    identifier : the source ip of the flow : int
    switch_list : list of IDs of all switches visited by this flow : list
    ratios : Dictionary, key -> ID of switch : str, value : ratio of packets 
    of that flow passing through switch to total packets passing through switch : float
    '''

    def __init__(self, identifier = None):
        if identifier is None:
            identifier = 0
        
        self.identifier = identifier
        self.switch_list = []
        self.ratios = {}
    
    def populateSwitchList(self, mysql_manager):
        '''
        Parameters: mysql_manager : Object of type MySQL_Manager class

        Purpose: Collects switch IDs of switches visited by the flow and
        populates the switch_list attribute.
        '''
        if mysql_manager is not None:
            result = mysql_manager.execute_query('select distinct switch from packetrecords where source_ip = ' + str(self.identifier))
            for row in result[1:]:
                self.switch_list.append(row[0])
    
    def populateRatios(self, mysql_manager):
        '''
        Parameters: mysql_manager : Object of type MySQL_Manager class
        
        Purpose: Calculates ratio of the flow in each switch it has visited
        and populates ratios attribute.
        '''
        if mysql_manager is not None:
            for switch in self.switch_list:
                result = mysql_manager.execute_query('select count(hash) from packetrecords where switch =\'' + switch + '\'')
                total_pkts = result[1][0]
                
                result = mysql_manager.execute_query('select count(hash) from packetrecords where switch =\'' + switch + '\'' + 'and source_ip = ' + str(self.identifier))
                self.ratios[switch] = result[1][0] / total_pkts

    def print_info(self, mapIp):
        '''
        Purpose: prints information about flow object
        '''

        print("Flow Identifier:" + ( str( mapIp[self.identifier] if self.identifier in mapIp else self.identifier ) ))

        print("Switches encountered:", end="")
        for switch in self.switch_list:
            print(switch, end=" ")
        
        print("\nRatios:")
        for switch in self.ratios:
            print("Switch " + switch + ": " + str(self.ratios[switch]))

        print("\n")

class QueryBuilder:
    '''
    A class to generate SQL query for Grafana based on certain
    parameters as specified during object creation.
    For details on querying Grafana using SQL, refer to below link:
    https://grafana.com/docs/grafana/latest/features/datasources/mysql/
    '''
    
    DEFAULT_TABLE = "packetrecords"
    DEFAULT_TIME_COLUMN = "time_in"

    def __init__(self, time_column = None, value = None, metricList = None, table = None, isAggregate = None, aggregateFunc = None, isConditional = None, conditionalClauseList = None):
        if time_column is None:
            time_column = QueryBuilder.DEFAULT_TIME_COLUMN
        if value is None:
            raise Error
        if metricList is None:
            raise Error
        if table is None:
            table = QueryBuilder.DEFAULT_TABLE
        if aggregateFunc is None:
            aggregateFunc = ""
            isAggregate = False
        if conditionalClauseList is None:
            conditionalClauseList = []
            isConditional = False
        
        self.time_column = time_column
        self.value = value
        self.metricList = metricList
        self.isAggregate = isAggregate
        self.aggregateFunc = aggregateFunc
        self.isConditional = isConditional
        self.conditionalClauseList = conditionalClauseList
        self.table = table
        
    def get_formatted_time(self, year):
        '''
        Parameters: year : str
        Returns a string in the format accepted by Grafana
        '''
        
        return "{}-{}-{}".format(year, "01", "01")

    def get_generic_query(self):
        '''
        Returns an SQL query according to specified paramers
        '''

        timeComponent = ""
        whereComponent = ""
        valueComponent = ""
        groupByComponent = ""
        metricComponent = ""

        if self.isConditional:
            whereComponent = "where "
            for clause in self.conditionalClauseList:
                whereComponent += (clause + " AND ")
            whereComponent = whereComponent[:-5] # Remove final AND

        if self.isAggregate:
            valueComponent = self.aggregateFunc + "(" + self.value + ")"
            groupByComponent = "group by 1,2"
        else:
            valueComponent = self.value

        timeComponent = self.time_column
        metricComponent = "concat("
        tableComponent = self.table
        for metric in self.metricList:
            metricComponent += '\'' + metric + ':\', ' + metric + "," + "\' \'" + ","
        metricComponent = metricComponent[:-5] # Remove the last comma and space
        metricComponent += ")"
        
        return "select {} as \'time\', {} as metric, {} FROM {} {} {} ORDER BY {}".format("from_unixtime(" + timeComponent + ")", metricComponent, valueComponent, tableComponent, whereComponent, groupByComponent, timeComponent)