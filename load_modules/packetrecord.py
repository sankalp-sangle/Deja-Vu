#!/usr/bin/env python


# Packet Record
# Pravein, 2019
# This class defines the elements in the PacketRecord

import re
import json

class packetrecord:
    def __init__(self, rawPacketRecord):
        self.packethash = 0
        self.packettime_in = 0
        self.packettime_queue = 0
        self.packet_stat = 0   # control plane version
        self.packet_stat2 = 0  # link utilization
        self.packetqueue_depth = 0
        self.packet_sip = 0
        self.parseRawPacketRecord(rawPacketRecord);

    def parseRawPacketRecord(self, rawPacketRecord):
        #print rawPacketRecord
        self.parsePacketHash(rawPacketRecord)
        self.parsePacketTimeIn(rawPacketRecord)
        self.parsePacketTimeQueue(rawPacketRecord)
        self.parsePacketStat(rawPacketRecord)
        self.parsePacketStat2(rawPacketRecord)
        self.parsePacketQueueDepth(rawPacketRecord)
        self.parsePacketSip(rawPacketRecord)
        self.to_str()
        pass

    def parsePacketHash(self, rawPacketRecord):
        packetHashIndex = 0
        for i in range(0,4):
            self.packethash = (self.packethash | int(rawPacketRecord[i], 16)) << 8
            #print int(rawPacketRecord[i], 16)
        self.packethash = self.packethash >> 8
        #print "Packet Hash : "+str(self.packethash)
        pass

    def parsePacketTimeIn(self, rawPacketRecord):
        packetTimeInIndex = 4
        for i in range(4,8):
            self.packettime_in = (self.packettime_in | int(rawPacketRecord[i], 16)) << 8
        self.packettime_in = self.packettime_in >> 8
        #print "Packet Time In : "+str(self.packettime_in)
        pass

    def parsePacketTimeQueue(self, rawPacketRecord):
        packetTimeQueueIndex = 8
        for i in range(8,12):
            self.packettime_queue = (self.packettime_queue | int(rawPacketRecord[i], 16)) << 8
        self.packettime_queue = self.packettime_queue >> 8
        #print "Packet Time Queue : "+str(self.packettime_queue)
        pass

    def parsePacketStat(self, rawPacketRecord):
        packetTimeQueueIndex = 12
        for i in range(12,16):
            self.packet_stat = (self.packet_stat | int(rawPacketRecord[i], 16)) << 8
        self.packet_stat = self.packet_stat >> 8
        #print "Packet Time Stat : "+str(self.packet_stat)
        pass

    def parsePacketStat2(self, rawPacketRecord):
        packetTimeQueueIndex = 16
        for i in range(16,20):
            self.packet_stat2 = (self.packet_stat2 | int(rawPacketRecord[i], 16)) << 8
            #print int(rawPacketRecord[i], 16)
        self.packet_stat2 = self.packet_stat2 >> 8
        #print "Packet Time Stat : "+str(self.packet_stat)
        # Calculate link util Mbps, stat2 is amount of bytes per 10us
        self.packet_stat2 = (self.packet_stat2 * 8)/10
        pass

    def parsePacketQueueDepth(self, rawPacketRecord):
        packetTimeQueueIndex = 20
        for i in range(20,24):
            self.packetqueue_depth = (self.packetqueue_depth | int(rawPacketRecord[i], 16)) << 8
            #print int(rawPacketRecord[i], 16)
        self.packetqueue_depth = self.packetqueue_depth >> 8
        #print "Packet Time Stat : "+str(self.packet_stat)
        pass

    def parsePacketSip(self, rawPacketRecord):
        packetTimeQueueIndex = 24
        for i in range(24,28):
            self.packet_sip = (self.packet_sip | int(rawPacketRecord[i], 16)) << 8
            #print int(rawPacketRecord[i], 16)
        self.packet_sip = self.packet_sip >> 8
        #print "Packet Time Stat : "+str(self.packet_stat)
        pass

    def to_str(self):
        print "Packet Time In : "+str(self.packettime_in) +", Packet Hash : "+str(self.packethash) +", Packet Time Queue : "+str(self.packettime_queue) +", Packet Time Stat : "+str(self.packet_stat)+", Packet Time Stat2 : "+str(self.packet_stat2)+", Packet Time SIP : "+str(self.packet_sip)

    def to_json(self):
        return dict(packetHash= self.packethash, packetTimeIn= self.packettime_in, packetTimeOut= self.packettime_in + self.packettime_queue, packetTimeQueue=self.packettime_queue, packetStat=self.packet_stat)
