from datetime import datetime

from .file import ModelFileHandle
from .db import normalize_b2_path

class NativeField:
    '''A field that requires no conversion'''

    def __init__(self, help=None):
        self.help = help

    is_b2db_field_type = True
    is_settable = True


    def format(self, record, attr_name, value):
        '''
        Format value to be saved in the record (written to JSON file)

        :param record: Instantiated Model representing a record value is being set on
        :param attr_name: Name of the attribute being set in the model
        :param value: Value being set
        :return: Value to store
        '''
        return value


    def parse(self, record, attr_name, value):
        '''
        Parse value back from parsed JSON read from record file

        :param record: Instantiated Model representing a record value is being read on
        :param attr_name: Name of the attribute being read in the model
        :param value: Value decoded from the JSON
        :return: Value to be used in the rest of the Program for this attribute
        '''
        return value



class CharField(NativeField): pass


class IntField(NativeField): pass


class DatetimeField(NativeField):
    STORE_DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'

    def __init__(self, help=None, microseconds=False):
        self.__ms = microseconds
        super().__init__(help=help)


    def format(self, record, attr_name, value):
        if value is not None:
            return value.strftime(DatetimeField.STORE_DATETIME_FORMAT)


    def parse(self, record, attr_name, value):
        if value:
            return datetime.strptime(value, DatetimeField.STORE_DATETIME_FORMAT)



class FileField(NativeField):
    '''Field to reference a file'''

    is_settable = False

    def __init__(self, upload_to=None, set_filename=None, use_version=True,
                 set_content_type=None, accept_content_types=None, help=None):
        '''
        :param upload_to: If set, specify folder to upload file to for this attribute
        :param set_filename: If set, always store filename as this
        :param use_version: If true, record points to a specific version of the file
        :param set_content_type: If set, always set file content type to this
        :param accept_content_types: If set, provide a list of content types accepted
        '''
        self.__use_version = use_version

        self.__upload_to = upload_to
        self.__set_filename = set_filename
        self.__set_content_type = set_content_type
        self.__accept_content_types = accept_content_types

        if self.__upload_to is not None:
            self.__upload_to = normalize_b2_path(self.__upload_to).strip('/')

        if self.__set_filename is not None:
            if '/' in normalize_b2_path(self.__set_filename):
                raise ValueError("Can't specify ")

        super().__init__(help)


    @property
    def use_version(self):
        return self.__use_version


    @property
    def upload_to(self):
        return self.__upload_to


    @property
    def set_filename(self):
        return self.__set_filename


    @property
    def set_content_type(self):
        return self.__set_content_type


    @property
    def accept_content_types(self):
        return self.__accept_content_types


    def parse(self, record, attr_name, value):
        '''
        Parse value back from parsed JSON read from record file

        :param record: Instantiated Model representing a record value is being read on
        :param attr_name: Name of the attribute being read in the model
        :param value: Value decoded from the JSON
        :return: Value to be used in the rest of the Program for this attribute
        '''
        return ModelFileHandle(
            record = record,
            attr_name = attr_name,
            value = value,
            attr_options = self)

