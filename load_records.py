#!/usr/bin/python


# NetPlay
# Pravein, 2019
# This utility is complement to Snaprr in providing network replay visualization
# with the collected packet records from all switches.

import re, sys
import time

import json

import mysql.connector
from pcapfile import savefile

# NetPlay libraries
from load_modules.switch import switch
from load_modules.link import link

def printUsage():
    print "Usage:  netplay.py  <Manifest File>"

STARTSWITCHES = "switches\n"
STARTLINKS   = "links\n"
STARTRECORDS  = "records\n"
STARTTRIGGER = "trigger"
STARTTORSWITCHES = "torswitches\n"

switchMacMap = {}
switchIdMap = {}
links = []
switchLabels = {}
torSwitches = []

trigger_id = ""
trigger_hit_time = 0
trigger_origin = None
# in ns
netPlayStartTime = 0xFFFFFFFF
netPlayEndTime = 0x0
netPlayInterval = 100


def processTorSwitch(myTorSwitchLine):
    str = myTorSwitchLine.split(",")
    switch_id = str[0].strip()
    torSwitches.append(switch_id)

def processSwitch(mySwitchLine):
    str = mySwitchLine.split(",")
    #print str
    switch_id = str[0].strip()
    switch_mac = str[1].strip()
    mySwitch = switch(switch_id, switch_mac)
    switchMacMap[switch_mac] = mySwitch
    switchIdMap[switch_id] = mySwitch
    switchLabels[switch_id] = switch_id
    pass
def processLink(myLinkLine):
    str = myLinkLine.split(",")
    #print str
    switch1 = str[0].strip()
    switch2 = str[1].strip()
    myLink = link(switch1, switch2)
    links.append(myLink)
    pass

def processTrigger(myTriggerLine):
    global trigger_id, trigger_hit_time, trigger_origin
    str = myTriggerLine.split(",")
    trigger_id = int(str[1])
    trigger_hit_time = int(str[2], 16)
    trigger_origin = switchMacMap[str[3].strip()]
    print trigger_origin
    pass

def processRecord(myRecordLine):
    str = myRecordLine.split(",")
    #print str
    if str[0].lower() == STARTTRIGGER:
        processTrigger(myRecordLine)
        return
    mySwitch = switchMacMap[str[0]]
    mySwitch.addRawRecord(str)



def f_comma(my_str, group=3, char=','):
    my_str = str(my_str)
    return char.join(my_str[i:i+group] for i in range(0, len(my_str), group))

COLLECT_PKT = "1237"
TRIGGER_PKT = "1236"

outputManifest = "./temp.mf"

def processPcap(manifestFile):
    print "Processing Pcap" + manifestFile
    myManifest = open(manifestFile, "rb")
    myOut = open(outputManifest,"w")
    capfile = savefile.load_savefile(myManifest, verbose=True)
    triggerDone = False
    print capfile

    myOut.write("Switches\n")
    myOut.write("1,000000000001\n")
    myOut.write("2,000000000002\n")
    myOut.write("3,000000000003\n");
    myOut.write("4,000000000004\n");
    myOut.write("5,000000000005\n");
    myOut.write("6,000000000006\n");
    myOut.write("7,000000000007\n");
    myOut.write("8,000000000008\n");
    myOut.write("9,000000000009\n");
    myOut.write("10,00000000000a\n");
    myOut.write("Links\n");
    myOut.write("1,3\n");
    myOut.write("1,4\n");
    myOut.write("2,3\n");
    myOut.write("2,4\n");
    myOut.write("3,5\n");
    myOut.write("4,5\n");
    myOut.write("6,8\n");
    myOut.write("6,9\n");
    myOut.write("7,8\n");
    myOut.write("7,9\n");
    myOut.write("8,10\n");
    myOut.write("9,10\n");
    myOut.write("3,10\n");
    myOut.write("4,10\n");
    myOut.write("8,5\n");
    myOut.write("9,5\n");
    myOut.write("Torswitches\n");
    myOut.write("1,000000000001\n")
    myOut.write("2,000000000002\n")
    myOut.write("6,000000000006\n");
    myOut.write("7,000000000007\n");
    myOut.write("Records\n");
    for pkt in capfile.packets:
        ethtype = pkt.packet[24:28]
        #print pkt.packet
        if ethtype == TRIGGER_PKT and triggerDone == False:
            #print pkt.packet
            src = pkt.packet[12:24]
            id = pkt.packet[35:36]
            time = pkt.packet[36:44]
            triggerstr =  "Trigger"+","+id+","+time+","+src
            triggerDone = True
            myOut.write(triggerstr+"\n")
        if ethtype == COLLECT_PKT:
            src = pkt.packet[12:24]
            entries = pkt.packet[42:44]
            precords = pkt.packet[46:len(pkt.packet)]
            precords_c = f_comma(precords, group=2)
            precordstr = src +","+ entries+","+ precords_c
            myOut.write(precordstr+"\n")
        #print ethtype
    pass

def processManifest(manifestFile):
    print "Processing " + manifestFile
    myManifest = open(manifestFile,"r")
    manifestState = -1
    for line in myManifest.readlines():
        # State Maintenance
        if line.lower() == STARTSWITCHES:
            manifestState = 0
            continue
        elif line.lower() == STARTLINKS:
            manifestState = 1
            continue
        elif line.lower() == STARTRECORDS:
            manifestState = 2
            continue
        elif line.lower() == STARTTORSWITCHES:
            manifestState = 3
            continue
        # Extract Sub-Components based on the state
        if manifestState == 0: # Switches
            processSwitch(line)
        elif manifestState == 1: # Links
            processLink(line)
        elif manifestState == 2: # Packet records
            processRecord(line)
        elif manifestState == 3: # Tor switch
            processTorSwitch(line)

class JsonEncoder(json.JSONEncoder):
    def default(self, obj):
        if hasattr(obj, 'to_json'):
            return obj.to_json()
        else:
            return json.JSONEncoder.default(self, obj)

class netplayJSON:
    def __init__(self, switchIdMap):
        self.switches = switchIdMap.values()

    def to_json(self):
        return dict(netplaySwitch=self.switches)

def encodeJson():
    # for myswitch in switchIdMap.values():
    #     print json.dumps(myswitch.to_json(), cls=JsonEncoder)
    netplayJ = netplayJSON(switchIdMap)
    print json.dumps(netplayJ.to_json(), cls= JsonEncoder, sort_keys=False, indent=4)

mysql_db = None

stat_name = "control_plane"
stat_name2 = "link_utilization"
stat_size = 32

def pushMySql(db_name):
    mysql_db = mysql.connector.connect(host="0.0.0.0", user="sankalp", passwd="sankalp")
    mycursor = mysql_db.cursor()
    try:
        mycursor.execute("DROP DATABASE " + db_name)
    except:
        pass
    mycursor.execute("CREATE DATABASE " + db_name)
    mycursor.execute("use " + db_name)
    table_create = "CREATE TABLE packetrecords (hash bigint, switch VARCHAR(255), time_in bigint, time_out bigint, time_queue int,"+stat_name + " bigint,"+stat_name2 + " bigint,"+ "queue_depth bigint, source_ip bigint)"
    mycursor.execute(table_create)
    table_create = "CREATE TABLE triggers (id int, time_hit bigint, switch VARCHAR(255))"
    mycursor.execute(table_create)
    table_create = "CREATE TABLE links (switch1 VARCHAR(255), switch2 VARCHAR(255), cap int)"
    mycursor.execute(table_create)
    table_create = "CREATE TABLE torswitches (switch VARCHAR(255))"
    mycursor.execute(table_create)

    table_insert = "INSERT INTO torswitches (switch) VALUES (%s)"
    for mytorswitch in torSwitches:
        val = (mytorswitch,)
        mycursor.execute(table_insert, val)
    table_insert = "INSERT INTO triggers (id, time_hit, switch) VALUES (%s, %s, %s)"
    val = (trigger_id, trigger_hit_time, trigger_origin.id)
    mycursor.execute(table_insert, val)
    table_insert = "INSERT INTO packetrecords (hash, switch, time_in, time_out, time_queue,"+stat_name +","+ stat_name2+",queue_depth,source_ip) VALUES (%s, %s, %s, %s, %s, %s, %s,%s,%s)"
    for myswitch in switchIdMap.values():
        for mypacketrecord in myswitch.precords:
            val = (mypacketrecord.packethash, str(myswitch.id), mypacketrecord.packettime_in, mypacketrecord.packettime_in+mypacketrecord.packettime_queue,mypacketrecord.packettime_queue, mypacketrecord.packet_stat, mypacketrecord.packet_stat2, mypacketrecord.packetqueue_depth, mypacketrecord.packet_sip)
            mycursor.execute(table_insert, val)
    table_insert = "INSERT INTO links (switch1, switch2, cap) VALUES (%s, %s, %s)"
    for mylink in links:
        val = (mylink.switch1, mylink.switch2, 10)
        mycursor.execute(table_insert, val)
        val = (mylink.switch2, mylink.switch1, 10)
        mycursor.execute(table_insert, val)
        mysql_db.commit()
    
#### Main
if len(sys.argv) < 2:
    printUsage()
else:
    manifestFile = str(sys.argv[1])
    if manifestFile.endswith('pcap'):
        processPcap(manifestFile)
        processManifest(outputManifest)
    else:
        processManifest(manifestFile)

    pushMySql(manifestFile[manifestFile.rindex("/")+1:-5])
