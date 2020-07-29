import re

from .file import normalize_b2_path

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


    def __getitem__(self, key):
        return self.get(key)


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


    def _upload_record_file(self, key, path, target, content_type=None):
        '''
        Upload a file into thestorage for the record (other than record data)

        :param key: Key to the record we're uploading the file file
        :param path: Path to the file on disk to upload
        :param target: Path relative to table storage root to save file to
        :param content_type: Content type to set on the file
        :return:  B2Obj object for uploaded file
        '''
        target = '/'.join((self.table_prefix, key, 'files', normalize_b2_path(target).strip('/')))

        return self.__db.upload_file(
            path = path,
            target = target,
            content_type = content_type)


    def _download_record_file_by_name(self, key, name, path):
        '''
        Download a file from record storage (other than record data)

        :param key: Key to the record to look in
        :param name: Path to file in B2 relative to record storage
        :param path: Path to download file to on disk
        :return:
            B2 file info.  Example:
            [fileId]:            '4_z9054c0781f4f079173300413_f116c051d4d126a9f_d20200720_m171424_c002_v0001133_t0012'
            [fileName]:          'unittests/test-0/people/sue.record'
            [contentType]:       'application/json'
            [contentLength]:     70
            [contentSha1]:       '2132096d380776b2ffc5dcb34d48a41cd5cfabdd'
            [fileInfo]:          {}
        '''
        name = '/'.join((self.table_prefix, key, 'files', normalize_b2_path(name).strip('/')))

        return self.__db.download_file(name = name, path = path)


    def _download_record_file_by_id(self, key, file_id, path):
        '''
        Download a file from record storage (other than record data)

        :param key: Key to the record to look in
        :param name: Path to file in B2 relative to record storage
        :param path: Path to download file to on disk
        :return:
            B2 file info.  Example:
            [fileId]:            '4_z9054c0781f4f079173300413_f116c051d4d126a9f_d20200720_m171424_c002_v0001133_t0012'
            [fileName]:          'unittests/test-0/people/sue.record'
            [contentType]:       'application/json'
            [contentLength]:     70
            [contentSha1]:       '2132096d380776b2ffc5dcb34d48a41cd5cfabdd'
            [fileInfo]:          {}
        '''
        return self.__db.download_file(file_id=file_id, path=path)

