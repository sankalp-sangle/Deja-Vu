DATABASE = "microburst_incast_heavyhitter3"
HOST            = "localhost"
URL             = "http://" + HOST + ":3000/api/dashboards/db"
ANNOTATIONS_URL = "http://" + HOST + ":3000/api/annotations"
DATASOURCE_URL  = "http://" + HOST + ":3000/api/datasources"
API_KEY         = "eyJrIjoiOFpNbWpUcGRPY3p2eVpTT0Iza0F5VzdNU3hJcmZrSVIiLCJuIjoibXlLZXkyIiwiaWQiOjF9"
YEAR_SEC = 31556926
UNIX_TIME_START_YEAR = 1970

headers = {
  'Accept': 'application/json',
  'Content-Type': 'application/json',
  'Authorization': 'Bearer ' + API_KEY
}