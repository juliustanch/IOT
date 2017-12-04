#!/usr/bin/env python
# -*- coding: utf-8 -*-
from SensorReader import ModbusSerialReader, AdcI2CReader

# Defining Dropbox's OA authentication keys (obtained from Dropbox developer website)
dropbox_keys = {
    'APP_KEY': '9ypojgeq2gvvbhc',
    'APP_SECRET':'bwqufbw61h3zji8',
    'TOKEN_KEY': '0il7v096buectimh',
    'TOKEN_SECRET': 'xj8uhbe7l2npnp8',
    'ACCESS_TYPE': 'dropbox',
    'APP_FOLDER': 'Prototype_App'
}

def PM3250_Setup():
    PM3250_reader = ModbusSerialReader(port='/dev/ttyAMA0',
                                       parity='E', 
                                       baudrate=19200,
                                       sensor_name='PM3250', 
                                       sensor_location='CCK', 
                                       dropbox_keys=dropbox_keys)
    input_read = PM3250_reader.read_input(input_filename='PM3250_input_settings.csv')
    PM3250_reader.start_collection(input_requests=input_read)


def SiSensor_Setup():
    SiSensor_reader = AdcI2CReader(bit='16-bit',
                                   pga=256,
                                   sps=8,
                                   sensor_name='SiSensor',
                                   sensor_location='CCK',
                                   dropbox_keys=dropbox_keys)
    input_read = [{'name': 'mini-volts', 'channel': 0}]
    SiSensor_reader.start_collection(input_requests=input_read)


def main():
    #PM3250_Setup()
    SiSensor_Setup()

if __name__ == '__main__':
    main()

