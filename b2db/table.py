

from .record import B2Record


class B2Table:
    '''
    Provide methods to work with a single table in a B2Database

    Note: This class doesn't store any state.  Just proxies methods on
    B2Database and generates records
    '''

    def __init__(self, database, table_name, record_class=B2Record):
        self.__db = database
        self.__name = table_name
        self.__record_class = record_class

