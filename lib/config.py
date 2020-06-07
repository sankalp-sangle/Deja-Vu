'''
Pending
'''

DATABASE = "microburst_incast_heavyhitter5"

HOST            = "localhost"
URL             = "http://" + HOST + ":3000/api/dashboards/db"
ANNOTATIONS_URL = "http://" + HOST + ":3000/api/annotations"
DATASOURCE_URL  = "http://" + HOST + ":3000/api/datasources"
API_KEY         = "eyJrIjoiOFpNbWpUcGRPY3p2eVpTT0Iza0F5VzdNU3hJcmZrSVIiLCJuIjoibXlLZXkyIiwiaWQiOjF9"
YEAR_SEC = 31556926
MAX_LEGAL_UNIX_TIMESTAMP = 2147383648
UNIX_TIME_START_YEAR = 1970
COLORS = ["#FF0000", "#00FF00", "#0000FF", "#FFFF00", "#FF00FF", "#00FFFF", "#FFFFFF", "#800000", "#008000", "#000080", "#808000", "#800080", "#008080", "#808080", "#C00000", "#00C000", "#0000C0", "#C0C000", "#C000C0", "#00C0C0", "#C0C0C0", "#400000", "#004000", "#000040", "#404000", "#400040", "#004040", "#404040", "#200000", "#002000", "#000020", "#202000", "#200020", "#002020", "#202020", "#600000", "#006000", "#000060", "#606000", "#600060", "#006060", "#606060", "#A00000", "#00A000", "#0000A0", "#A0A000", "#A000A0", "#00A0A0", "#A0A0A0", "#E00000", "#00E000", "#0000E0", "#E0E000", "#E000E0", "#00E0E0", "#E0E0E0"]
MAX_WIDTH = 1000000 # Nanoseconds
LEFT_THRESHOLD = 0.3
RIGHT_THRESHOLD = 0.5
CLEANUP_QUERIES = ['set SQL_SAFE_UPDATES = 0', 'delete from packetrecords where time_in = 0', 'delete from packetrecords where source_ip = 1543569666']

headers = {
  'Accept': 'application/json',
  'Content-Type': 'application/json',
  'Authorization': 'Bearer ' + API_KEY
}