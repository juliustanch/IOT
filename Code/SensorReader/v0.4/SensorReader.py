#!/usr/bin/env python
# -*- coding: utf-8 -*-
# title           :  SensorReader.py
# description     :  A module to manage sensors connection
# author          :  Gary Goh <garygsw@gmail.com>
# date            :  3/6/2015
# version         :  0.4
# notes           :  Most compatible in Linux OS
# python_version  :  2.7.8
# =============================================================================


# Import the modules needed to run the script
import spi
import RPi.GPIO as GPIO
import psycopg2
from math import sqrt
from os import makedirs
from os.path import isdir, isfile
from time import sleep, time
from datetime import datetime
from pymodbus.constants import Endian
from pymodbus.payload import BinaryPayloadDecoder
from pymodbus.client.sync import ModbusSerialClient
from dropbox import client, session
from urllib3 import disable_warnings
from threading import Thread
from Adafruit.ADS1x15.Adafruit_ADS1x15 import ADS1x15 as AdcReader


class AdcRtd_Client(object):
    """
    Customized class for a client with both ADC and RTD reading functions

    """
    def __init__(self, adc_client, rtd_client):
        self.adc_client = adc_client
        self.rtd_client = rtd_client

    def read_adc(self, channel, pga, sps):
        return self.adc_client.readADCSingleEnded(channel, pga, sps)

    def read_rtd(self):
        return self.rtd_client.pullf()

    def test_connection(self):
        # Need to implement: check adc connection
        # Need to implement: check rtd connection
        return True


class RTD_Client(object):
    """
    Custom-made class client for RTD SPI.

    """

    a = 0.00390830
    b = -0.0000005775
    c = -0.00000000000418301
    rtdR = 400
    rtd0 = 100

    # Registers
    __REG_CONFIGURATION = 0x00
    __REG_RTD_MSB = 0x01
    __REG_RTD_LSB = 0x02
    __REG_HF_MSB = 0x03
    __REG_HF_LSB = 0x04
    __REG_LF_MSB = 0x05
    __REG_LF_LSB = 0x06
    __REG_FAULT_STATUS = 0x07

    # Configuration options
    __REG_CONF_50HZ_FILTER = (1 << 0)
    __REG_CONF_FAULT_STATUS_AUTO_CLEAR = (1 << 1)
    __REG_CONF_3WIRE_RTD = (0 << 4)
    __REG_CONF_1SHOT = (1 << 5)
    __REG_CONF_CONVERSION_MODE_AUTO = (1 << 6)
    __REG_CONF_VBIAS_ON = (1 << 7)

    def __init__(self, bus, channel, _3wire=True):
        device = "/dev/spidev%s.%s" % (bus, channel)

        spi.openSPI(speed=100000, mode=1, device=device)

        self.config = self.__REG_CONF_VBIAS_ON | self.__REG_CONF_50HZ_FILTER | self.__REG_CONF_CONVERSION_MODE_AUTO
        if(_3wire):
            self.config |= self.__REG_CONF_3WIRE_RTD

        self.__write__(self.__REG_CONFIGURATION, self.config | self.__REG_CONF_FAULT_STATUS_AUTO_CLEAR)
        self.__write__(self.__REG_LF_MSB, 0x00)
        self.__write__(self.__REG_LF_LSB, 0x00)
        self.__write__(self.__REG_HF_MSB, 0xFF)
        self.__write__(self.__REG_HF_LSB, 0xFF)

    def __read__(self, address):
        assert (address >= 0 and address <= 0x07)
        return spi.transfer((address, 0))[1]

    def __write__(self, address, n):
        assert (address >= 0 and address <= 0x07)
        assert (n >= 0 and n <= 0xFF)
        spi.transfer((address | 0x80, n))

    def pullf(self):
        msb_rtd = self.__read__(self.__REG_RTD_MSB)
        lsb_rtd = self.__read__(self.__REG_RTD_LSB)
        rtdRaw = ((msb_rtd << 7) + ((lsb_rtd & 0xFE) >> 1))
        rtdT = (rtdRaw * self.rtdR) / 32768.
        temp = -self.rtd0 * self.a + sqrt(self.rtd0 ** 2 * self.a ** 2 - 4 * self.rtd0 * self.b * (self.rtd0 - rtdT))
        temp = temp / (2 * self.rtd0 * self.b)
        return temp


class BaseReader(object):
    """
    The base class of all sensor readers to manage read, save and upload operations.

    Attributes:

        sensor_name (string):
            Name of the sensor/device to be read and managed.

        sensor_location (string):
            Name of the location of the sensor/device.

        delimiter (string):
            The seperator sequence to be used in the output file.

        dropbox_settings (dict[string]):
            Dropbox account settings, which includes OAauthentication keys.

        database_settings (dict[string]):
            Database settings, which includes authentication information.

        dropbox_client (dropbox.client):
            Dropbox connection client.

        header_row (string):
            The first row of the output file to denote the header.

        output_filename (string):
            The name of the output file of the current day.

        output_file_ext (string):
            The file extension of the output file.

        output_file_folder (string):
            The name of the folder to store the outputs in the current directory.

    Compulsory Implementation:  # specifics to each connection type

        initialize_connection(args*):
            Initialize the sensor connection client object.

        test_connection()
            Returns true if the sensor connection is valid; false otherwise.

        execute_requests(List[request])
            Returns a list of sensor reading values.

    """
    # Initializing class attributes
    sensor_name = None
    sensor_location = None
    delimiter = None
    dropbox_settings = None
    database_settings = None
    dropbox_client = None
    header_row = None
    output_filename = None
    output_file_ext = None
    output_file_folder = None

    def __init__(self, sensor_name='sensor', sensor_location='location',
                 dropbox_keys=None, database_keys=None, delimiter=',',
                 output_file_ext='.csv', output_file_folder='Outputs'):
        """
        Constructor method of the BaseReader class.
        The main attributes of the sensor is initalized.

        Parameters:
            Argument 1: sensor_name (string, optional)
                The name of the sensor/device.
                Defaults to "sensor".

            Argument 2: sensor_location (string, optional)
                The name of the sensor/device location.
                Defaults to "location".

            Argument 3: dropbox_keys (dict[key:string, value:string], optional)
                The authentication information for dropbox upload.
                Defaults to None.
                Dictionary format: {'APP_KEY': string,
                                    'APP_SECRET': string,
                                    'TOKEN_KEY': string,
                                    'TOKEN_SECRET': string,
                                    'ACCESS_TYPE': string,
                                    'APP_FOLDER': string }

            Argument 4: database_keys (dict[key:string, value:string], optional)
                The database connection settings.
                Defaults to None.
                Dictionary format: {'HOST': string,
                                    'USER': string,
                                    'PASSWORD': string,
                                    'DBNAME': string,
                                    'DBTABLE': string }

            Argument 5: delimiter (string, optional):
                The seperator sequence to be used in the output file.
                Defaults to "," (comma).

            Argument 6: output_file_ext (string, optional):
                The file extension of the output file.
                Defaults to ".csv" (comma separated values).

            Argument 7: output_file_folder (string, optional):
                The name of the output folder in the current directory.
                Defaults to "Output".

        Raises:

            TypeError: If any type of the inputs is invalid.
                       sensor_name or sensor_location is invalid; length < 0 or >=50, or with delimiters.

            ValueError: If any format of the inputs is invalid.

        """
        # Input type validation
        if type(sensor_name) is not str:
            raise TypeError('The name of sensor_name must be a string')
        if type(sensor_location) is not str:
            raise TypeError('The name of sensor_location must be a string')
        if type(delimiter) is not str:
            raise TypeError('The delimiter must be a string')
        if type(output_file_ext) is not str:
            raise TypeError('The output file extension must be a string')
        if type(output_file_folder) is not str:
            raise TypeError('The output file folder name must be a string')

        # Input value validation
        if len(sensor_name) == 0 or len(sensor_name) >= 50 or sensor_name.find(delimiter) != -1:
            raise ValueError('Invalid sensor name; enter a string of length 1-50 with no "' + delimiter + '"')
        if len(sensor_location) == 0 or len(sensor_location) >= 50 or sensor_location.find(delimiter) != -1:
            raise ValueError('Invalid sensor location; enter a string of length 1-50 with no "' + delimiter + '"')
        if len(delimiter) == 0 or len(delimiter) > 1:
            raise ValueError('Invalid delimiter value')
        if len(output_file_ext) < 1 or len(output_file_ext) > 10:
            raise ValueError('Invalid output file extension value; must have at 1-8 charaters')
        if output_file_ext[0] != '.':
            raise ValueError('Output file extension value must starts with "."')
        if len(output_file_folder) == 0 or len(output_file_folder) >= 50:
            raise ValueError('Output file folder invalid; must 1-50 characters')

        # Initialize base reader parameters
        self.sensor_name = sensor_name
        self.sensor_location = sensor_location
        self.delimiter = delimiter
        self.output_file_ext = output_file_ext
        self.output_file_folder = output_file_folder

        # Create output folder in current directory if it does not exist
        if not isdir(output_file_folder):
            makedirs(output_file_folder)

        # Initialize dropbox settings if given (if not given, dropbox upload is disabled)
        if dropbox_keys is not None:
            if type(dropbox_keys) is not dict:
                raise TypeError('The type of dropbox_keys must be in a dictionary')
            check_keys = ['APP_KEY', 'APP_SECRET', 'TOKEN_KEY', 'TOKEN_SECRET', 'ACCESS_TYPE', 'APP_FOLDER']
            if not all(type(key) is str for key in dropbox_keys.keys()):
                raise TypeError('The format of dropbox_keys keys is invaid; must be string')
            if not all(key in dropbox_keys.keys() for key in check_keys):
                raise ValueError('Some key(s) in dropbox_keys is invalid; please check again')
            if not all(type(dropbox_keys[key]) is str for key in check_keys):
                raise ValueError('The format of dropbox_keys values is invalid; must be string')
            self.dropbox_settings = {key: dropbox_keys[key] for key in check_keys}

        # Initialize database settings if given (if not given, database update is disabled)
        if database_keys is not None:
            if type(database_keys) is not dict:
                raise TypeError('The type of database_keys must be in a dictionary')
            check_keys = ['HOST', 'USER', 'PASSWORD', 'DBNAME', 'DBTABLE']
            if not all(type(key) is str for key in database_keys.keys()):
                raise TypeError('The format of database_keys keys is invaid; must be string')
            if not all(key in database_keys.keys() for key in check_keys):
                raise ValueError('Some key(s) in database_keys is invalid; please check again')
            if not all(type(database_keys[key]) is str for key in check_keys):
                raise ValueError('The format of database_keys values is invalid; must be string')
            self.database_settings = {key: database_keys[key] for key in check_keys}

    def get_output_row(self, requests, print_output):
        """
        Compiles the read requests together into a single row.
        Also uploads row to database (if enabled)

        Parameters:

            Argument 1: input_requests (List[dict])
                Contains a list of requests to be executed.

            Argument 2: print_output (bool)
                Whether or not to print the output to console after read complete.

        Returns:

            row_string (string):
                A single row of outputs.

        """
        # Get current time stamp and add it to output row
        time_stamp = datetime.now().strftime('%H:%M:%S')
        date_stamp = datetime.now().strftime('%y%m%d')
        output = [date_stamp, time_stamp]

        for request in requests:
            result = self.execute_request(request)
            if print_output:
                print request[self.KEYS['NAME']], result
            output.append(str(result))
        row_string = self.delimiter.join(output) + '\n'

        # Save row into database; if enabled
        if self.database_settings is not None:
            self.upload_to_database(output)

        return row_string

    def read_all_requests(self, requests, print_output):
        """
        Executes a list of requests and save the output to an output file.

        Parameters:

            Argument 1: input_requests (List[dict])
                Contains a list of requests to be executed.

            Argument 2: print_output (bool)
                Whether or not to print the output to console after read complete.

        """
        start_read_time = time()
        print 'Reading data on ' + str(datetime.now()) + '...'
        with open(self.output_filename, 'ab+') as output_file:  # ab+: append-only in binary, backward readable
            if output_file.readline() != self.header_row:  # if header does not exist / new file
                output_file.write(self.header_row)
            row_string = self.get_output_row(requests, print_output)
            output_file.write(row_string)
        end_read_time = time()
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

    def upload_to_dropbox(self, filename):
        """
        Uploads the filename of the local output file and upload it to dropbox app folder.

        Parameters:

            Argument 1: filename (string)
                The name of the file to be uploaded onto Dropbox.

        Returns:

            Upload time (float): The amount of time it takes to upload the file onto dropbox.

        """
        try:
            start_upload_time = time()
            print 'Uploading "' + filename + '" on ' + str(datetime.now()) + '...'
            with open(filename) as output_file:
                self.dropbox_client.put_file('/' + self.dropbox_settings['APP_FOLDER'] + '/' + filename,
                                             output_file,
                                             overwrite=True)
            end_upload_time = time()
            upload_time = end_upload_time - start_upload_time
            print 'Completed: Uploaded "' + filename + '"" to dropbox in ' + "%.2f" % upload_time + 's.'
            return upload_time
        except Exception as exception:
            print 'Dropbox upload of "' + filename + '"" unsuccessful at ' + str(datetime.now()) + ';'
            print 'Reason of upload failure: ' + str(type(exception).__name__)

    def start_dropbox_upload(self, interval):
        """
        Initiates the dropbox upload cycle.
        Each dropbox upload is executed on a separate thread.

        Parameters:

            Argument 1: interval (float)
                The file upload interval (in seconds).

        """
        self.initialize_dropbox_client()
        while True:
            self.wait_for_interval(interval)
            upload_thread = Thread(target=self.upload_to_dropbox, args=(self.output_filename,))
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

    def update_output_file(self, previous_date):
        """
        To keep track if a new day arrives; updates the output filename if new day.

        Parameters:

            Argument 1: previous_date (string)
                The string of the previous date to compare to.
                Format: YYMMDD

        """
        previous_output_filename = self.output_filename
        date_stamp = datetime.now().strftime('%y%m%d')
        if date_stamp != previous_date:
            self.output_filename = '_'.join([date_stamp, self.sensor_name, self.sensor_location])
            self.output_filename += self.output_file_ext
            self.output_filename = self.output_file_folder + '/' + self.output_filename

        # A new day has arrived; need to upload the dropbox of the previous day immediately
        if previous_date is not None:
            self.upload_to_dropbox(filename=previous_output_filename)

    def start_collection(self, input_requests, read_interval=60, upload_interval=300,
                         print_output=False, zip_output_file=False):
        """
        Starts the data collection process and repeat while the connection is still valid.
        The output file is also uploaded to dropbox folder (if upload is enabled).

        Parameters:

            Argument 1: input_requests (List[dict])
                Contains a list of requests represented by dictionaries.

            Argument 2: read_interval (float, optional)
                The data reading interval (in seconds).
                Defaults to 60s, or 1 minute.

            Argument 3: upload_interval (float, optional)
                The data upload to dropbox interval (in seconds).
                Defaults to 300s, or 5 minutes.

            Argument 4: print_output (bool, optional)
                Whether or not to print the output to console after read complete.
                Defaults to False.

            Argument 5: zip_output_file (bool, optional)
                Whether or not the output file is to be zipped or not.
                Defaults to False.

        Notes:

            The format of the output filename is:

                [YYMMDD]_[sensor name]_[sensor location][FILE_EXT]

            The format of the output is:

                [Date stamp], [Time stamp], [Request 1], [Request 2] ...
                YYMMDD, HH:MM:SS, Format 1, Format 2, ...

        Raises:

            TypeError: If any of the input types is invaid.

            SystemError: If the connection of the Modbus client is invalid.

            ValueError: If the interval values are invalid.

        """
        # Validation
        if not self.test_connection():
            raise SystemError('Connection with device failed; retry connection using initialize_connection()')
        if type(read_interval) is not int:
            raise TypeError('The value of the read interval must be an integer')
        if read_interval > 86400 or read_interval <= 0:  # 86400 = 24 hrs = 1 day
            raise ValueError('Invalid read interval value; enter a value between 1-86399')
        if self.dropbox_settings:
            if type(upload_interval) is not int:
                raise TypeError('The value of the upload interval must be an integer')
            if upload_interval < read_interval:
                raise ValueError('Invalid upload interval value; enter a value >= read interval')
        if type(print_output) is not bool:
            raise TypeError('The type of print_output must be a bool')

        # Initialize parameters
        previous_date = None
        self.header_row = self.get_header(input_requests)
        print 'Data collection started...'

        # Wait until the next minute starts
        self.wait_next_minute()

        # Initialize dropbox upload cycle
        if self.dropbox_settings:
            upload_thread = Thread(target=self.start_dropbox_upload, args=(upload_interval,))
            upload_thread.start()

        # Update the file once
        update_file_thread = Thread(target=self.update_output_file, args=(previous_date,))
        update_file_thread.start()

        # Repeat data collection until connection is lost or operation abort
        try:
            while True:
                update_file_thread = Thread(target=self.update_output_file, args=(previous_date,))
                update_file_thread.start()
                previous_date = datetime.now().strftime('%y%m%d')

                # Initiate read all requests on a separate thread
                read_thread = Thread(target=self.read_all_requests, args=(input_requests, print_output))
                read_thread.start()

                # Wait for a pre-defined read interval
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

    def __init__(self, port, parity, baudrate, timeout=0.05,
                 sensor_name='sensor', sensor_location='location',
                 dropbox_keys=None, database_keys=None, delimiter=',',
                 output_file_ext='.csv', output_file_folder='Outputs'):
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

            Argument 4: timeout (float, optional)
                The amount of time given for the client to read per request.
                Defaults to 0.05.

            Argument 5, 6, 7, 8, 9, 10, 11: See BaseReader

        """
        super(ModbusSerialReader, self).__init__(sensor_name=sensor_name,
                                                 sensor_location=sensor_location,
                                                 dropbox_keys=dropbox_keys,
                                                 database_keys=database_keys,
                                                 delimiter=delimiter,
                                                 output_file_ext=output_file_ext,
                                                 output_file_folder=output_file_folder)
        self.initialize_connection(port, parity, baudrate, timeout)

    def initialize_connection(self, port, parity, baudrate, timeout):
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
            timeout=timeout
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

    def __init__(self, bit, pga, sps, sensor_name='sensor',
                 sensor_location='location', dropbox_keys=None,
                 database_keys=None, delimiter=',',
                 output_file_ext='.csv', output_file_folder='Outputs'):
        """
        Constructor method of the AdcI2CReader class.
        The Adc client is initialized.

        Parameters:

            Argument 1: bit (string)
                The type of Adc bit; Either only 12-bit or 16-bit.

            Argument 2: pga (int)
                The resolution of the reading.

            Argument 3: sps (int)
                Sample rate; samples per second.

            Argument 4, 5, 6, 7, 8, 9, 10: See BaseReader

        """
        super(AdcI2CReader, self).__init__(sensor_name=sensor_name,
                                           sensor_location=sensor_location,
                                           dropbox_keys=dropbox_keys,
                                           database_keys=database_keys,
                                           delimiter=delimiter,
                                           output_file_ext=output_file_ext,
                                           output_file_folder=output_file_folder)
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
        if type(bit) is not str:
            raise TypeError('The format of bit must be a string; either "12-bit" or "16-bit"')
        if bit not in ['12-bit', '16-bit']:
            raise ValueError('The value bit must be either "12-bit" or "16-bit"')
        if type(pga) is not int:
            raise TypeError('The format of pga must be an integer')  # Need to know the range
        if type(sps) is not int:
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


class RtdSPIReader(BaseReader):
    """"
    A class to manage RTD SPI connections.

    Constants:

        KEYS (dict[string]):
            Defines the dictionary keys of a RTD SPI request.

    Attributes:

        client (RTD_Client):
            Object of the RTD client.

        RTD_Client:
            Class for RTD SPI reader

    """
    # Defining the dictionary keys of a RTD SPI request
    KEYS = {
        'NAME': 'name',
    }

    client = None

    def __init__(self, bus, channel, sensor_name='sensor',
                 sensor_location='location', dropbox_keys=None,
                 database_keys=None, delimiter=',',
                 output_file_ext='.csv', output_file_folder='Outputs'):
        """
        Constructor method of the RtdSPIReader class.
        The Rtd client is initialized.

        Parameters:

            Argument 1: bus (int)
                The bus number of the SPI connection.

            Argument 2: channel(int)
                The channel number of the SPI connection.

            Argument 3, 4, 5, 6, 7, 8, 9: See BaseReader

        """
        super(RtdSPIReader, self).__init__(sensor_name=sensor_name,
                                           sensor_location=sensor_location,
                                           dropbox_keys=dropbox_keys,
                                           delimiter=delimiter,
                                           database_keys=database_keys,
                                           output_file_ext=output_file_ext,
                                           output_file_folder=output_file_folder)
        self.initialize_connection(bus, channel)

    def initialize_connection(self, bus, channel):
        """
        Defines and set the class attribute for the Rtd client with
        the settings given in the parameters.

        Parameters:

            Argument 1: bus (int)
                The bus number of the SPI connection.

            Argument 2: channel(int)
                The channel number of the SPI connection.

        Raises:

            TypeError: If the type of any of the inputs is invalid.

            ValueError: If the value of any of the inputs is invalid.

        """
        # Validation
        if type(bus) is not int:
            raise TypeError('The type of bus number must be an integer')
        if type(channel) is not int:
            raise TypeError('The type of channel number must be an integer')
        if bus != 0:  # need to find out the limit
            raise ValueError('Invalid bus number')
        if channel < 0 or channel > 1:
            raise ValueError('Invalid channel number')

        GPIO.setmode(GPIO.BOARD)
        GPIO.setup(16, GPIO.IN)
        self.client = RTD_Client(bus, channel)

    def test_connection(self):
        """
        Checks and reports the status of the Rtd SPI connection.

        Returns:
            True: If the connection is valid;
            False: If the connection is not valid.

        Raises:
            SystemError: If the Rtd SPI client is yet to be initialized.

        """
        if self.client is None:
            raise SystemError('RTD SPI client not yet initialize; run initialize_connection() first')
        return True  # Implement how to check SPI connection

    def execute_request(self, request):
        """
        Executes a reading request.

        Parameters:

            Argument 1: request (dict)
                Not required in this case.

        Returns:

            Reading (type: any):
                The decoded data parsed in the data type of the format as given.

        """
        return self.client.pullf()


class IrradianceReader(BaseReader):
    """"
    A class to manage the connection to a irradiance sensor.

    Constants:

        KEYS (dict[string]):
            Defines the dictionary keys of a combined request.

    Attributes:

        client (AdcRtdClient):
            Object of the Irridiance client.

        temperature (float):
            The temperature of the sensor in degrees Celsius.

        mini_voltage (float):
            The mini-voltage of the reading.

    """
    # Defining the dictionary keys of an irradiance sensor request
    KEYS = {
        'NAME': 'name',
        'TYPE': 'type',
        'CHANNEL': 'channel',
    }

    client = None
    temperature = None
    mini_voltage = None

    def __init__(self, adc_bit, adc_pga, adc_sps,
                 rtd_bus, rtd_channel, sensor_name='sensor',
                 sensor_location='location', dropbox_keys=None,
                 database_keys=None, delimiter=',',
                 output_file_ext='.csv', output_file_folder='Outputs'):
        """
        Constructor method of the IrradianceReader class.
        The irradiance sensor client is initialized.

        Parameters:

            Argument 1: adc_bit (string)
                The type of Adc bit; Either only 12-bit or 16-bit.

            Argument 2: adc_pga (int)
                The resolution of the reading.

            Argument 3: adc_sps (int)
                Sample rate; samples per second.

            Argument 4: rtd_bus (int)
                The bus number of the RTD.

            Argument 5: rtd_channel(int)
                The channel number of the RTD.

            Argument 6, 7, 8, 9, 10, 11, 12: See BaseReader

        """
        super(IrradianceReader, self).__init__(sensor_name=sensor_name,
                                               sensor_location=sensor_location,
                                               dropbox_keys=dropbox_keys,
                                               delimiter=delimiter,
                                               database_keys=database_keys,
                                               output_file_ext=output_file_ext,
                                               output_file_folder=output_file_folder)
        self.initialize_connection(adc_bit, adc_pga, adc_sps, rtd_bus, rtd_channel)

    def initialize_connection(self, adc_bit, adc_pga, adc_sps, rtd_bus, rtd_channel):
        """
        Defines and set the class attribute for the AdcRtd client with
        the settings given in the parameters.

        Parameters:

            Argument 1: adc_bit (string)
                The type of ADC bit; Either only 12-bit or 16-bit.

            Argument 2: adc_pga (int)
                The resolution of the reading.

            Argument 3: adc_sps (int)
                Sample rate; samples per second.

            Argument 4: rtd_bus (int)
                The bus number of the RTD.

            Argument 5: rtd_channel(int)
                The channel number of the RTD.

        Raises:

            TypeError: If the type of any of the inputs is invalid.

            ValueError: If the value of any of the inputs is invalid.

        """
        # Validation
        if type(adc_bit) is not str:
            raise TypeError('The format of adc_bit must be a string; either "12-bit" or "16-bit"')
        if adc_bit not in ['12-bit', '16-bit']:
            raise ValueError('The value adc_bit must be either "12-bit" or "16-bit"')
        if type(adc_pga) is not int:
            raise TypeError('The format of adc_pga must be an integer')  # Need to know the range
        if type(adc_sps) is not int:
            raise TypeError('The format of adc_sps must be an integer')  # Need to know the range
        if type(rtd_bus) is not int:
            raise TypeError('The type of rtd_bus number must be an integer')
        if type(rtd_channel) is not int:
            raise TypeError('The type of rtd_channel number must be an integer')
        if rtd_bus != 0:  # need to find out the limit
            raise ValueError('Invalid bus number')
        if rtd_channel < 0 or rtd_channel > 1:
            raise ValueError('Invalid rtd_channel number')

        # Initialize ADC client
        if adc_bit == '12-bit':
            ic = 0x00  # ADS1015
        else:
            ic = 0x01  # ADS1115
        self.pga = adc_pga
        self.sps = adc_sps
        adc_client = AdcReader(ic=ic)

        # Initialize RTD client
        GPIO.setmode(GPIO.BOARD)
        GPIO.setup(16, GPIO.IN)
        rtd_client = RTD_Client(rtd_bus, rtd_channel)

        # Initialize into one combined client
        self.client = AdcRtd_Client(adc_client=adc_client, rtd_client=rtd_client)

    def test_connection(self):
        """
        Checks and reports the status of the AdcRtd connection.

        Returns:
            True: If the connection is valid;
            False: If the connection is not valid.

        Raises:
            SystemError: If the client is yet to be initialized.

        """
        if self.client is None:
            raise SystemError('AdcRtd client not yet initialize; run initialize_connection() first')
        return self.client.test_connection()

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
        if request[self.KEYS['TYPE']] == 'I2C':
            self.mini_voltage = self.client.read_adc(request[self.KEYS['CHANNEL']], self.pga, self.sps)
            return self.mini_voltage
        if request[self.KEYS['TYPE']] == 'SPI':
            self.temperature = self.client.read_rtd()
            return self.temperature
        if request[self.KEYS['TYPE']] == 'derived':
            irradiance = self.mini_voltage / 54.57 * 1000 / (1 + 0.0005 * (self.temperature - 25))
            return irradiance

    def upload_to_database(self, output):
        """
        Executes the upload of the entry to the database.

        Parameters:

            Argument 1: output (List[string])
                The output of a request represented by a list of values.

        """

        db_conn = None
        try:
            # Define database connection string
            conn_string = "host='%s' user='%s' password='%s' dbname='%s'" % (self.database_settings['HOST'],
                                                                             self.database_settings['USER'],
                                                                             self.database_settings['PASSWORD'],
                                                                             self.database_settings['DBNAME'])
            db_conn = psycopg2.connect(conn_string)
            cursor = db_conn.cursor()
            insert_string = "INSERT INTO %s (%s) VALUES ('%s', '%s', %s, %s, %s)" % (self.database_settings['DBTABLE'],
                                                                                     self.header_row.strip(),
                                                                                     output[0],
                                                                                     output[1],
                                                                                     output[2],
                                                                                     output[3],
                                                                                     output[4])
            cursor.execute(insert_string)
            db_conn.commit()
        except Exception as exception:
            print 'Database upload unsuccessful at ' + str(datetime.now()) + ';'
            print 'Reason of upload failure: ' + str(type(exception).__name__)
        finally:
            if db_conn is not None:
                db_conn.close()
