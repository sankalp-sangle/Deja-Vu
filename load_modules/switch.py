#!/usr/bin/env python


# Switch
# Pravein, 2019
# This class defines the elements in the Switch

import re
from packetrecord import packetrecord
from packetevent import packetevent
from enum import Enum

class switch:
    def __init__(self, switch_id, switch_mac):
        self.id = switch_id
        self.mac = switch_mac
        self.precords = []
        self.packetevents = []
        self.packeteventInMap = {}
        self.playPointer = 0

    def addRawRecord(self, rawRecord):
        rawRecordSize = 28 # 2 stats, sip and queue depth
        #rawRecordSize = 16
        print "Adding Raw record"
        #print rawRecord
        num_entries = int(rawRecord[1], 16)
        print "num_entries =" + rawRecord[1]
        rawPacketRecordIndex = 2
        for i in range(0,num_entries):
            print "Adding packet record"
            rawPacketRecord = rawRecord[rawPacketRecordIndex:rawPacketRecordIndex + rawRecordSize]
            #print rawPacketRecord
            packrecord = packetrecord(rawPacketRecord)

            self.precords.append(packrecord)
            rawPacketRecordIndex = rawPacketRecordIndex + rawRecordSize
        pass

    def sortPacketRecords(self):
        # print "Switch ID :"+ self.id
        # for mypacketrecords in self.precords:
        #     mypacketrecords.to_str()
        self.precords.sort(key = lambda x: x.packettime_in)
        # print "Sorted Packet Records"
        # for mypacketrecord in self.precords:
        #     mypacketrecord.to_str()
        for mypacketrecord in self.precords:
            # TODO : Identify Ingress / Egress Stat
            packetevent_in = packetevent(PacketEventType.In, mypacketrecord.packettime_in, mypacketrecord.packethash, None)
            packettime_out = mypacketrecord.packettime_in + mypacketrecord.packettime_queue
            packetevent_out = packetevent(PacketEventType.Out, packettime_out, mypacketrecord.packethash, mypacketrecord.packet_stat)
            self.packetevents.append(packetevent_in)
            self.packetevents.append(packetevent_out)
            self.packeteventInMap[mypacketrecord.packethash] = packetevent_in
        self.packetevents.sort(key = lambda x: x.event_time)
        # for mypacketevent in self.packetevents:
        #     mypacketevent.to_str()
        if len(self.packetevents) == 0:
            return 0,0
        return self.packetevents[0].event_time, self.packetevents[len(self.packetevents)-1].event_time
        pass

    def play(self, netPlayCurTime):
        print "Switch "+ self.id
        for pointer in range(self.playPointer, len(self.packetevents)):
            if self.packetevents[pointer].elapsed(netPlayCurTime):
                self.packetevents[pointer].to_str()
            else:
                break
            self.playPointer += 1
        pass

    def getPacketEvents(self, netPlayCurTime):
        packetEvents = []
        for pointer in range(self.playPointer, len(self.packetevents)):
            if self.packetevents[pointer].elapsed(netPlayCurTime):
                packetEvents.append(self.packetevents[pointer])
            else:
                break
            self.playPointer += 1
        return packetEvents
        pass

    def findPacketIn(self, packetHash):
        print self.packeteventInMap

    def to_json(self):
        return dict(packetrecords = self.precords, switch_id=self.id)

class PacketEventType(Enum):
    In = 1
    Out = 2
