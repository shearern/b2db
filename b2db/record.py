


class B2Record:
    '''
    A single record in the B2Database

    All calls that access data are proxied back the B2Database
    Use [] to access the raw data stored in the record
    Use .attributes to access decoded data
    '''

    def __init__(self, table, key, init_data=None):
        self.__table = table
        self.__key = key
        self.__data = init_data



