#!/usr/bin/env python
# -*- coding: utf-8 -*-
# title           :  SensorReader.py
# description     :  A module to manage sensors connection
# author          :  Gary Goh <garygsw@gmail.com>
# date            :  22/5/2015
# version         :  0.1
# notes           :  Most compatible in Linux OS
# python_version  :  2.7.8
# =============================================================================


# Import the modules needed to run the script
from os.path import isfile, join, dirname
from time import sleep, time
from datetime import datetime
from pymodbus.constants import Endian
from pymodbus.payload import BinaryPayloadDecoder
from pymodbus.client.sync import ModbusSerialClient
from dropbox import client, session
from urllib3 import disable_warnings
from threading import Thread
from Adafruit.ADS1x15.Adafruit_ADS1x15 import ADS1x15 as AdcReader


class BaseReader(object):
    """
    The base class of all sensor readers to manage read, save and upload operations.
    
    Constants:
    
        FILE_EXT (string):
            The default file extension of the output file.
    
    Attributes:
    
        sensor_name (string):
            Name of the sensor/device to be read and managed.

        sensor_location (string):
            Name of the location of the sensor/device.

        delimiter (string):
            The seperator sequence to be used in the output file.

        dropbox_settings (dict[string]):
            Dropbox account settings, which includes OAauthentication keys
        
        dropbox_client (dropbox.client):
            Dropbox connection client.
            
        header_row (string):
            The first row of the output file to denote the header.
            
        output_filename (string):
            The name of the output file of the current day.
            
    Need Implementation:  # specifics to each connection type
    
        initialize_connection(args)
        
        test_connection()
    
        execute_requests(List[request])

    """
    # Defining output file extension
    FILE_EXT = '.csv'
    
    # Initializing class attributes
    sensor_name = None
    sensor_location = None
    delimiter = None
    dropbox_settings = None
    dropbox_client = None
    header_row = None
    output_filename = None
    
    def __init__(self, sensor_name='sensor', sensor_location='location', 
                 dropbox_keys=None, delimiter=','):
        """
        Constructor method of the BaseReader class.
        The attributes of the sensor is initalized and the dropbox client is initialized.

        Parameters:
            Argument 1: sensor_name (string, optional)
                    The name of the sensor/device.
                    Defaults to "sensor".

            Argument 2: sensor_location (string, optional)
                The name of the sensor/device location.
                Defaults to "location".
                
            Argument 3: dropbox_keys (dict[string], optional)
                The authentication information for dropbox upload.
                Defaults to None.
                
                Dictionary format: {'APP_KEY': string,
                                    'APP_SECRET': string,
                                    'TOKEN_KEY': string,
                                    'TOKEN_SECRET': string,
                                    'ACCESS_TYPE': string,
                                    'APP_FOLDER': string }
                                    
            Argument 4: delimiter (string, optional):
                The seperator sequence to be used in the output file.
                Defaults to "," (comma).

        Raises:

            TypeError: If any type of the inputs is invalid.
                       sensor_name or sensor_location is invalid; length < 0 or >=50, or with delimiters.
            
            ValueError: If any format of the inputs is invalid.

        """
        # Validation
        if type(sensor_name) != str:
            raise TypeError('The name of sensor_name must be a string')
        if type(sensor_location) != str:
            raise TypeError('The name of sensor_location must be a string')
        if type(delimiter) != str:
            raise TypeError('The value of the delimiter must be a string')
        if delimiter == '' or len(delimiter) > 1:
            raise ValueError('Invalid delimiter value')
        if sensor_name == '' or len(sensor_name) >= 50 or sensor_name.find(delimiter) != -1:
            raise ValueError('Invalid sensor name; enter a string of length 1-50 with no "' + delimiter + '"')
        if sensor_location == '' or len(sensor_location) >= 50 or sensor_location.find(delimiter) != -1:
            raise ValueError('Invalid sensor location; enter a string of length 1-50 with no "' + delimiter + '"')
        self.sensor_name = sensor_name
        self.sensor_location = sensor_location
        self.delimiter = delimiter
        
        # Initialize dropbox settings if given (if not given, dropbox upload is disabled)
        if dropbox_keys is not None:
            if type(dropbox_keys) != dict:
                raise ValueError('The type of dropbox_keys must be in a dictionary')
            check_keys = ['APP_KEY', 'APP_SECRET', 'TOKEN_KEY', 'TOKEN_SECRET', 'ACCESS_TYPE', 'APP_FOLDER']
            if not all(key in dropbox_keys.keys() for key in check_keys):
                raise ValueError('The format of dropbox_keys is invaid')
            self.dropbox_settings = {key: dropbox_keys[key] for key in check_keys}

    def read_all_requests(self, requests):
        """
        Executes a list of requests and save the output to an output file.

        Parameters:

            Argument 1: input_requests (List[])
                Contains a list of requests to be executed.

        """
        time_stamp = datetime.now().strftime('%H:%M:%S')
        date_stamp = datetime.now().strftime('%d%m%y')
        output = [date_stamp, time_stamp]

        # Read all requests and write to output file
        with open(self.output_filename, 'ab+') as output_file:  # ab+: append-only in binary, backward readable
            if output_file.readline() != self.header_row:  # if header does not exist
                output_file.write(self.header_row)
                
            start_read_time = time()
            print 'Reading data on ' + str(datetime.now()) + '...'
            for request in requests:
                output.append(str(self.execute_request(request)))
            end_read_time = time()
            
            row_string = self.delimiter.join(output) + '\n'
            output_file.write(row_string)
        
        read_time = end_read_time - start_read_time
        print 'Completed: Read in ' + "%.2f" % read_time + 's.'

    
    def initialize_dropbox_client(self):
        """
        Initializes the dropbox connection via a client session, using pre-defined authenticaion keys
        The client is then set within the class attribute.
        Visit dropbox developer website to obtain OAuthentication keys.

        """
        sess = session.DropboxSession(self.dropbox_settings['APP_KEY'], 
                                      self.dropbox_settings['APP_SECRET'], 
                                      self.dropbox_settings['ACCESS_TYPE'])
        sess.set_token(self.dropbox_settings['TOKEN_KEY'], self.dropbox_settings['TOKEN_SECRET'])
        self.dropbox_client = client.DropboxClient(sess)
        disable_warnings()

    def upload_to_dropbox(self):
        """
        Uploads the filename of the local output file and upload it to dropbox app folder.
                
        Returns:
        
            Upload time (float): The amount of time it takes to upload the file onto dropbox.

        """
        try:
            start_upload_time = time()
            print 'Uploading "' + self.output_filename + '" on ' + str(datetime.now()) + '...'
            with open(self.output_filename) as output_file:
                self.dropbox_client.put_file('/' + self.dropbox_settings['APP_FOLDER'] + '/' + self.output_filename,
                                             output_file, 
                                             overwrite=True)
            end_upload_time = time()
            upload_time = end_upload_time - start_upload_time
            print 'Completed: Uploaded "' + self.output_filename + '"" to dropbox in ' + "%.2f" % upload_time + 's.'
            return upload_time
        except Exception as exception:
            print 'Dropbox upload of "' + self.output_filename + '"" unsuccessful at ' + str(datetime.now()) + ';'
            print 'Reason of upload failure: ' + str(type(exception).__name__)

    def start_dropbox_upload(self, interval):
        """
        Initiates the dropbox upload cycle.

        Parameters:

            Argument 1: interval (float)
                The file upload interval (in seconds).
                
        """
        self.initialize_dropbox_client()
        while True:
            self.wait_for_interval(interval)
            # Initiate upload on a separate thread
            upload_thread = Thread(target=self.upload_to_dropbox)
            upload_thread.start()

    def wait_for_interval(self, interval, threshold=900000):
        """
        Waits for a period in seconds round up to the nearest microseconds.
        Programmed to be precise with error of +0.02s.

        Parameters:

            Argument 1: interval (float)
                The data reading interval (in seconds).

            Argument 2: threshold (int, optional)
                The number of microseconds which as as a threshold which if
                the current time microseconds exceeds, if will skip the first sleep.
                Defaults to 900000.

        """
        sleep(interval - 0.5)  # sleep until the last 0.5 second
        microsecs = datetime.utcnow().microsecond
        if microsecs < threshold:
            sleep((threshold - microsecs) / 1000000.)
        while datetime.utcnow().microsecond >= threshold:
            pass

    def wait_next_minute(self, threshold=57):
        """
        Waits until the next nearest minute at the 0th second.
        Programmed to be precise with error of +0.02s.

        Parameters:

            Argument 1: threshold (int, optional)
                The number of seconds which as as a threshold which if
                the current time seconds exceeds, if will skip the first sleep.
                Defaults to 57.

        """
        seconds = datetime.utcnow().second
        if seconds < threshold:
            sleep(threshold - seconds)
        while datetime.utcnow().second >= threshold:
            pass
    
    def get_header(self, requests):
        """
        Prepares the header of the output file in the required format.

        Parameters:

            Argument 1: requests (List[dict])
                A list of requests represeted by dictionaries.

        Notes:

            The format of the header is:

                [Date stamp], [Time stamp], [Request 1], [Request 2] ...

        Returns:

            Header (string):
                The header represented by a series of text joined by delimiters, and endline at the end

        """
        output = 'date_stamp' + self.delimiter + 'time_stamp' + self.delimiter
        output += self.delimiter.join([request[self.KEYS['NAME']] for request in requests])
        return output + '\n'
    
    def start_collection(self, input_requests, read_interval=60, upload_interval=300):
        """
        Starts the data collection process and repeat while the connection is still valid.
        The output file is also uploaded to dropbox folder.

        Parameters:

            Argument 1: input_requests (List[dict])
                Contains a list of requests represented by dictionaries.

            Argument 2: read_interval (float)
                The data reading interval (in seconds).
                Defaults to 60s, or 1 minute.

            Argument 3: upload_interval (float)
                The data upload to dropbox interval (in seconds).
                Defaults to 300s, or 5 minutes.

        Notes:

            The format of the output filename is:

                [YYMMDD]_[sensor name]_[sensor location][FILE_EXT]

            The format of the output is:

                [Date stamp], [Time stamp], [Request 1], [Request 2] ...
                DDMMYY, HH:MM:SS, Format 1, Format 2, ...       

        Raises:
        
            TypeError: If any of the input types is invaid.

            SystemError: If the connection of the Modbus client is invalid.

            ValueError: If the interval values are invalid.

        """
        # Validation
        if type(read_interval) != int:
            raise TypeError('The value of the read interval must be an integer')
        if read_interval > 86400 or read_interval <= 0:  # 86400 = 24 hrs = 1 day
            raise ValueError('Invalid read interval value; enter a value between 1-86399')
        if not self.test_connection():
            raise SystemError('Connection with device failed; retry connection using initialize_connection()')
        if self.dropbox_settings:
            if type(upload_interval) != int:
                raise TypeError('The value of the upload interval must be an integer')
            if upload_interval < read_interval:
                raise ValueError('Invalid upload interval value; enter a value >= read interval')

        # Initialize parameters
        previous_date = None
        self.header_row = self.get_header(input_requests)
        print 'Data collection started...'
        
        self.wait_next_minute()

        # Initialize dropbox upload cycle
        if self.dropbox_settings:
            upload_thread = Thread(target=self.start_dropbox_upload, args=(upload_interval,))
            upload_thread.start()

        try:
            while True:  # repeat until connection is lost
                # To keep track if a new day arrives; updates the output filename if new day
                date_stamp = datetime.now().strftime('%y%m%d')
                if date_stamp != previous_date:
                    self.output_filename = '_'.join([date_stamp, self.sensor_name, self.sensor_location]) + self.FILE_EXT
                previous_date = date_stamp

                # Initiate read all requests on a separate thread
                read_thread = Thread(target=self.read_all_requests, args=(input_requests,))
                read_thread.start()

                self.wait_for_interval(read_interval)
        except Exception as exception:
            print 'Data collection ended...'
            if not self.test_connection():
                raise SystemError('Connection with device failed; retry connection using initialize_connection()')
            else:
                print 'Reason of failure: ' + str(type(exception).__name__)


class ModbusSerialReader(BaseReader):
    """"
    A class to manage Mobus serial connections.

    Constants:
    
        KEYS (dict[string]):
            Defines the dictionary keys of a Modbus request.
    
        __TYPES (List[string]):
            A list of all data types that can be read.
            
        __REGISTER_COUNT_BY_TYPE (dict[int]):
            Defines the mapping of input type to registry size.
            
        __DECODER_BY_TYPE (dict[lambda]):
            Defines the mapping of decoding functions.
            

    Attributes:

        client (pymodbus.client.sync.ModbusSerialClient):
            Object of the ModbusSerialClient class.

    """
    # Defining the dictionary keys of a Modbus request
    KEYS = {
        'NAME': 'name',
        'SLAVE_UNIT': 'slave_unit',
        'ADDRESS': 'address',
        'TYPE': 'type',
    }
    
    # Defining types codes
    __TYPES = ['UInt16', 'Int16', 'UInt32', 'Int64', 'UTF8', 'Float32']

    # Defining the mapping of input type to registry size
    __REGISTER_COUNT_BY_TYPE = {
        'UInt16': 1,
        'Int16': 1,
        'UInt32': 2,
        'Int64': 4,
        'UTF8': 8,
        'Float32': 2,
    }

    # Defining the mapping of decoding functions
    __DECODER_BY_TYPE = {
        'UInt16': lambda x: BinaryPayloadDecoder.fromRegisters(x, endian=Endian.Big).decode_16bit_uint(),
        'Int16': lambda x: BinaryPayloadDecoder.fromRegisters(x, endian=Endian.Big).decode_16bit_int(),
        'UInt32': lambda x: BinaryPayloadDecoder.fromRegisters(x, endian=Endian.Big).decode_32bit_uint(),
        'Int64': lambda x: BinaryPayloadDecoder.fromRegisters(x, endian=Endian.Big).decode_64bit_int(),
        'UTF8': lambda x: BinaryPayloadDecoder.fromRegisters(x, endian=Endian.Big).decode_string(8),
        'Float32': lambda x: BinaryPayloadDecoder.fromRegisters(x, endian=Endian.Big).decode_32bit_float(),
    }
    
    client = None

    def __init__(self, port, parity, baudrate, 
                 sensor_name='sensor', sensor_location='location', dropbox_keys=None, delimiter=','):
        """
        Constructor method of the ModbusSerialReader class.
        The Modbus client is initialized.

        Parameters:

            Argument 1: port (string)
                The address of the port that is connected to the device.

            Argument 2: parity (string)
                The parity setting of the connection.

            Argument 3: baudrate (int)
                The rate of bits transfer per second.
                
            Argument 4, 5, 6, 7: See BaseReader

        """
        super(ModbusSerialReader, self).__init__(sensor_name=sensor_name,
                                                 sensor_location=sensor_location,
                                                 dropbox_keys=dropbox_keys)
        self.initialize_connection(port, parity, baudrate)

    def initialize_connection(self, port, parity, baudrate):
        """
        Defines and set the class attribute for the Modbus serial client with 
        the settings given in the parameters.

        Parameters:

            Argument 1: port (string)
                The address of the port that is connected to the device.

            Argument 2: parity (string)
                The parity setting of the connection.
                Must either be one of "E", "O" or "N" only.

            Argument 3: baudrate (int)
                The rate of bits transfer per second.
                Must be in the possible values of 300-100000.

        Raises:

            ValueError: If parity code is invalid, or baudrate is invalid; < 300 or > 1000000.
            
            SystemError: If connection with the device fails.

        """
        # Validation
        if parity not in 'EON' or len(parity) != 1:
            raise ValueError('Invalid parity code; enter either "E", "O" or "N"')
        if baudrate < 300 or baudrate > 100000:
            raise ValueError('Invalid baudrate; enter possible values of 300-100000')

        self.client = ModbusSerialClient(
            method='rtu',
            port=port,
            stopbits=1,
            bytesize=8,
            parity=parity,
            baudrate=baudrate,
            timeout=0.05
        )
        if not self.test_connection():
            raise SystemError('Connection with device failed; retry connection using initialize_connection()')

    def test_connection(self):
        """
        Checks and reports the status of the serial Modbus connection.

        Returns:
            True: If the connection is valid;
            False: If the connection is not valid.

        Raises:
            SystemError: If the Modbus client is yet to be initialized.
        
        """
        if self.client is None:
            raise SystemError('Modbus client not yet initialize; run initialize_connection() first')
        return self.client.connect()

    def read_input(self, input_filename, has_headers=True, delimiter=','):
        """
        Reads a list of input settings from a file in the local directory.

        Parameters:

            Argument 1: input_filename (string)
                The name of the input file to be read. 

            Argument 2: has_headers (bool, optional)
                True if input file has headers; false otherwise.
                Defaults to True.

            Argument 3: delimiter (string, optional)
                A separator sequence to denote end of data read.
                Defaults to "," (comma).

        Note:

            The required format of the input file is:

                [Name of data] [Slave unit], [Starting register address], [Data type]

        Returns:

            List of dictionary (List[dict]): 
                List of requests represented in dictionary form.
                e.g.  [ { 'name': 'apparent_power', 'slave_unit': '1', 'address': '3053', 'type': 'Float32' },
                          { 'name': 'real_power', 'slave_unit': '1', 'address': '3053', 'type': 'Float32' },
                          { 'name': 'total_power', 'slave_unit': '1', 'address': '3053', 'type': 'Float32' } ]

        Raises:

            SystemError: If the input fie does not exist.
            
            TypeError: If type code is invalid.

            ValueError: If address is invalid range; < 0, or > 39999 or
                                slave_unit, address is not an integer.

        """
        # Validation
        # file_path = join(os.path.dirname(__file__), input_filename)
        if not isfile(input_filename):
            raise SystemError('Input file "' + input_filename + '" does not exist')
        
        # Reads in the data
        with open(input_filename, 'rb') as input_file:  # rb: read-only in binary
            header = [self.KEYS['NAME'], self.KEYS['SLAVE_UNIT'], self.KEYS['ADDRESS'], self.KEYS['TYPE']]
            if has_headers:
                output = [dict(zip(header, line.strip().split(delimiter))) for line in input_file][1:]  # remove first row
            else:
                output = [dict(zip(header, line.strip().split(delimiter))) for line in input_file]

        # To convert types and data validation
        for request in output:
            request[self.KEYS['SLAVE_UNIT']] = int(request[self.KEYS['SLAVE_UNIT']])  # for slave unit
            request[self.KEYS['ADDRESS']] = int(request[self.KEYS['ADDRESS']])  # for address
            if request[self.KEYS['ADDRESS']] < 0 or request[self.KEYS['ADDRESS']] > 39999:
                raise ValueError('Address "' + str(request[self.KEYS['ADDRESS']]) + '" not within possible range: 0-39999')
            if request[self.KEYS['TYPE']] not in self.__TYPES:  # for type
                raise TypeError('Type code "' + request[self.KEYS['TYPE']] + '" entered is invalid')

        return output
    
    def execute_request(self, request):
        """
        Executes a reading request.

        Parameters:

            Argument 1: request (dict)
                The request represented by a dictionary as prepared by the input reader.

        Returns:

            Reading (type: any):
                The decoded data parsed in the data type of the format as given.

        Raises:

            KeyError: If any of the keys of the dictionary is invalid.

            SystemError: If the connection of the Modbus client is invalid.

        """
        # Validation
        if not self.test_connection():
            raise SystemError('Connection with device failed; retry connection using initialize_connection()')

        # Read raw values from holding registers
        response = self.client.read_holding_registers(
            address=request[self.KEYS['ADDRESS']],
            count=self.__REGISTER_COUNT_BY_TYPE[request[self.KEYS['TYPE']]],
            unit=request[self.KEYS['SLAVE_UNIT']]
        )

        return self.decode(response, request[self.KEYS['TYPE']])

    def decode(self, response, data_type):
        """
        Decodes the response according to the data format given.

        Parameters:

            Argument 1: response (List[int])
                The response of the read represented by a list of 16-bit int values in a list.

            Argument 2: data_type (string)
                The format of the data to be decoded.

        Returns:

            Reading (type: any):
                The decoded data parsed in the data type of the format as given.
                
        Raises:
        
            ValueEror: If the output is invalid.

        """
        try:
            return self.__DECODER_BY_TYPE[data_type](response.registers)
        except:
            return 0
            raise ValueError('Invalid output decoded')


class AdcI2CReader(BaseReader):
    """"
    A class to manage ADC I2C connections.
    
    Constants:
            
        KEYS (dict[string]):
            Defines the dictionary keys of a ADC I2C request.

    Attributes:

        client (Adafruit.ADS1x15.Adafruit_ADS1x15.ADS1x15):
            Object of the Adc client.

    """
    # Defining the dictionary keys of a ADC I2C request
    KEYS = {
        'NAME': 'name',
        'CHANNEL': 'channel',
    }
    
    client = None

    
    def __init__(self, bit, pga, sps,
                 sensor_name='sensor', sensor_location='location', dropbox_keys=None, delimiter=','):
        """
        Constructor method of the ModbusSerialReader class.
        The Adc client is initialized.

        Parameters:

            Argument 1: bit (string)
                The type of Adc bit; Either only 12-bit or 16-bit.

            Argument 2: pga (int)
                The resolution of the reading.

            Argument 3: sps (int)
                Sample rate; samples per second.
                
            Argument 4, 5, 6, 7: See BaseReader

        """
        super(AdcI2CReader, self).__init__(sensor_name=sensor_name,
                                           sensor_location=sensor_location,
                                           dropbox_keys=dropbox_keys)
        self.initialize_connection(bit, pga, sps)
        
    def initialize_connection(self, bit, pga, sps):
        """
        Defines and set the class attribute for the Adc client with 
        the settings given in the parameters.
        
        Parameters:

            Argument 1: bit (string)
                The type of Adc bit; Either only 12-bit or 16-bit.

            Argument 2: pga (int)
                The resolution of the reading.

            Argument 3: sps (int)
                Sample rate; samples per second.
                
        Raises:
            
            TypeError: If the type of any of the inputs is invalid.
            
            ValueError: If the value of any of the inputs is invalid.
        
        """
        # Validation
        if type(bit) != str:
            raise TypeError('The format of bit must be a string; either "12-bit" or "16-bit"')
        if bit not in ['12-bit', '16-bit']:
            raise ValueError('The value bit must be either "12-bit" or "16-bit"')
        if type(pga) != int:
            raise TypeError('The format of pga must be an integer')  # Need to know the range 
        if type(sps) != int:
            raise TypeError('The format of sps must be an integer')  # Need to know the range 
        
        # Initialize values
        if bit == '12-bit':
            ic = 0x00  # ADS1015
        else:
            ic = 0x01  # ADS1115
        self.pga = pga
        self.sps = sps
        
        self.client = AdcReader(ic=ic)
        
    def test_connection(self):
        """
        Checks and reports the status of the Adc I2C connection.

        Returns:
            True: If the connection is valid;
            False: If the connection is not valid.

        Raises:
            SystemError: If the Adc I2C client is yet to be initialized.
        
        """
        if self.client is None:
            raise SystemError('ADC I2C client not yet initialize; run initialize_connection() first')
        return True  # Implement how to check I2C connection
    
    def execute_request(self, request):
        """
        Executes a reading request.

        Parameters:

            Argument 1: request (dict)
                The request represented by a dictionary as prepared by the input reader.

        Returns:

            Reading (type: any):
                The decoded data parsed in the data type of the format as given.

        Raises:

            KeyError: If any of the keys of the dictionary is invalid.
            
        """
        return self.client.readADCSingleEnded(request[self.KEYS['CHANNEL']], self.pga, self.sps)
