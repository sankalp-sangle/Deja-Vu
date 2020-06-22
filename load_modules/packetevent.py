#!/usr/bin/env python


# Packet Record
# Pravein, 2019
# This class defines the elements in the PacketRecord

import re
class packetevent:
    def __init__(self, type, event_time, packethash, packet_stat):
        self.type = type
        self.event_time = event_time
        self.packethash = packethash
        self.packet_stat = packet_stat

    def elapsed (self, curTime):
        if self.event_time < curTime:
            return True
        return False

    def to_str (self):
        print "Event Type"+str(self.type) +", Packet Hash : "+str(self.packethash) +", Time : "+str(self.event_time) +", Packet Stat : "+str(self.packet_stat)
