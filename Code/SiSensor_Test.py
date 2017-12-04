#!/usr/bin/env python
# -*- coding: utf-8 -*-
from SensorReader import IrradianceReader

# Defining Dropbox's OA authentication keys (obtained from Dropbox developer website)
dropbox_keys = {
    'APP_KEY': '9ypojgeq2gvvbhc',
    'APP_SECRET': 'bwqufbw61h3zji8',
    'TOKEN_KEY': '0il7v096buectimh',
    'TOKEN_SECRET': 'xj8uhbe7l2npnp8',
    'ACCESS_TYPE': 'dropbox',
    'APP_FOLDER': 'Prototype_App'
}

# Defining PostgreSQL connection settings
postgresql_keys = {
    'HOST': '137.132.165.224',
    'USER': 'postgres',
    'PASSWORD': 'Seris@1212',
    'DBNAME': 'readings',
    'DBTABLE': 'irradiance_reading'
}


def SiSensor_Setup():
    SiSensor_reader = IrradianceReader(adc_bit='16-bit',
                                       adc_pga=256,
                                       adc_sps=860,
                                       rtd_bus=0,
                                       rtd_channel=0,
                                       sensor_name='SiSensor',
                                       sensor_location='CCK',
                                       dropbox_keys=dropbox_keys,
                                       database_keys=postgresql_keys)
    input_read = [{'name': 'mini_volts', 'type': 'I2C', 'channel': 0},
                  {'name': 'temperature', 'type': 'SPI'},
                  {'name': 'irradiance', 'type': 'derived'}]  # must be placed last
    SiSensor_reader.start_collection(input_requests=input_read, read_interval=60)


def main():
    SiSensor_Setup()

if __name__ == '__main__':
    main()
