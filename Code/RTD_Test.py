#!/usr/bin/env python
# -*- coding: utf-8 -*-
from SensorReader import RtdSPIReader

# Defining Dropbox's OA authentication keys (obtained from Dropbox developer website)
DROPBOX_KEYS = {
    'APP_KEY': '9ypojgeq2gvvbhc',
    'APP_SECRET':'bwqufbw61h3zji8',
    'TOKEN_KEY': '0il7v096buectimh',
    'TOKEN_SECRET': 'xj8uhbe7l2npnp8',
    'ACCESS_TYPE': 'dropbox',
    'APP_FOLDER': 'Prototype_App'
}

def SiSensorRtd_Setup():
    SiSensorRtd_reader = RtdSPIReader(bus=0,
                                      channel=0,
                                      sensor_name='SiSensorRtd', 
                                      sensor_location='CCK', 
                                      dropbox_keys=DROPBOX_KEYS)
    input_read = [{'name': 'temperature'}]
    SiSensorRtd_reader.start_collection(input_requests=input_read)

def main():
    SiSensorRtd_Setup()

if __name__ == '__main__':
    main()

