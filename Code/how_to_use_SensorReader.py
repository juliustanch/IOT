#!/usr/bin/env python
# -*- coding: utf-8 -*-

# to read RTD
from SensorReader import RtdSPIReader

SPIsensor_reader = RtdSPIReader(bus=0, channel=0)
spi_request = {'name': 'reading_name'}
output = SPIsensor_reader.execute_request(spi_request)
print output


# to read ADC
from SensorReader import AdcI2CReader

I2Csensor_reader = AdcI2CReader(bit='16-bit', pga=256, sps=860)
i2c_request = {'name': 'reading_name', 'channel': 0}
output = I2Csensor_reader.execute_request(i2c_request)
print output


# to read Modbus
from SensorReader import ModbusSerialReader

Modbus_reader =ModbusSerialReader(port='/dev/ttyAMA0', parity='E', baudrate=19200)
modbus_request = {'name': 'reading_name', 'slave_unit': 1, 'register_address': 3053, 'type': 'Float32'}
output = Modbus_reader.execute_request(modbus_request)
print output


# to read Irradiance (both ADC and RTD) ; pls don't mind my spelling error, will change later
from SensorReader import IrridianceReader

Irridiance_sensor = IrridianceReader(adc_bit='16-bit', adc_pga=256, adc_sps=860, rtd_bus=0, rtd_channel=0)

mini_volts_request = {'name': 'mini_volts', 'type': 'I2C', 'channel': 0}
mini_volts = Irridiance_sensor.execute_request(mini_volts_request)
print mini_volts

temperature_request = {'name': 'temperature', 'type': 'SPI'}
temperature = Irridiance_sensor.execute_request(temperature_request)
print temperature

irradiance_request ={'name': 'irradiance', 'type': 'derived'}
irradiance = Irridiance_sensor.execute_request(irradiance_request)
print irradiance

# to begin the reading logging
# input is a list of dictionaries; add more request in the list in order to read additional readings
SPIsensor_reader.start_collection(input_requests=[spi_request])
I2Csensor_reader.start_collection(input_requests=[i2c_request])
Modbus_reader.start_collection(input_requests=[modbus_request])
Irridiance_sensor.start_collection(input_requests=[mini_volts_request, temperature_request, irradiance_request])
