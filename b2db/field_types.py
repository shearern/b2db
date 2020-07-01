from datetime import datetime

class B2FieldTypes:
    '''Formatters to write and read attribute types'''


    STORE_DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'

    @staticmethod
    def format_datetime(value):
        if value is not None:
            return value.strftime(B2FieldTypes.STORE_DATETIME_FORMAT)

    @staticmethod
    def prase_datetime(value):
        if value:
            return datetime.strptime(value, B2FieldTypes.STORE_DATETIME_FORMAT)
