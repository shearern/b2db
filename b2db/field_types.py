from datetime import datetime


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






