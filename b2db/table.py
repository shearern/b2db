import re

def normalize_key(key):
    '''Change key so that it doesn't interfere with cloud storage structure'''
    return key.replace("\\", '/').strip('/').replace('/', '__')


class Table:
    '''
    Provide methods to work with a single table in a B2Database

    Note: This class doesn't store any state.  Just proxies methods on
    B2Database and generates records.
    '''

    def __init__(self, db, model_attr_name, model):
        '''
        :param db: Reference to instantiated Database to access cloud storage
        :param model_attr_name: Name of the attriblte in the DB holding this Model class
        :param model: Model class (not instantiated) for data stored in this table
        '''
        self.__db = db
        self.__model_attr_name = model_attr_name
        self.__model = model


    @property
    def table_prefix(self):
        try:
            return self.__model.Meta.table_prefix
        except AttributeError:
            return self.__model_attr_name


    def create(self, key, **init_values):
        '''
        Create a new record of the
        :param key:
        :param init_values:
        :return:
        '''
        record = self.__model(table=self, key=key)

        # Set attributes as if being changed (as opposed to being loaded back from
        # storage, so changed = True)
        for name, value in init_values.items():
            record.__setattr__(name, value)

        return record


    def _record_prefix(self, key):
        '''
        Calc path that all files for a given record should be saved under

        :param key: Key to identify record
        :return: Path to folder to store record related files DB root
        '''
        return '/'.join((self.table_prefix, normalize_key(key)))


    def _record_data_path(self, key):
        '''
        Path to store JSON data for record data

        :param key: Key to identify record
        :return: Path to file relative to DB root
        '''
        return '/'.join((self._record_prefix(key), 'record-data.json'))


    def write_record_data(self, key, save_data):
        '''
        Write record data to storage for this table

        :param key: Key to identify record to write
        :param save_data: Record data to write (dict)
        '''

        path = self._record_data_path(key)
        self.__db.write_record_data(path, save_data)



    def read_record_data(self, key):
        '''
        Get record data from storage for this table

        :param key: Key to identify record to write
        :return: Record data decoded from file (dict)
        '''
        path = self._record_data_path(key)
        return self.__db.read_record_data(path)


    def get(self, key):
        '''Get a record by it's key'''
        saved_data = self.read_record_data(key)
        return self.__model(table=self, key=key, saved_data=saved_data)


    def delete(self, key):
        '''Delete a record from a table'''
        path = self._record_prefix(key)
        self.__db.delete_record_files(path)


    def list_keys(self):
        '''List the keys in a table'''
        return self.__db.list_table_keys(self.table_prefix)


    def all(self):
        '''List all records in the table'''
        for key in self.list_keys():
            yield self.get(key)
