import os
import json
import re

from b2sdk.v1 import B2Api
from b2sdk.v1 import InMemoryAccountInfo, SqliteAccountInfo
from b2sdk.v1 import DownloadDestBytes
from b2sdk.exception import FileNotPresent

from .table import Table


class NotAModelAttribute(Exception): pass


class B2Database(object):
    '''
    A Database of records and files stored in BackBlaze.

    Intended to be subclassed to define the structure of your own database

    Class represents the structure of a DB
    Instance represents a specific instance of the DB with credentials to access
    '''

    def __init__(self, bucket, b2_key_id, b2_secret, b2_auth_cache_path=None, prefix=None):
        '''
        :param bucket: Bucket name to use
        :param b2_key_id: B2 key to authenticate with
        :param b2_secret: B2 secret to authenticate with
        :param b2_auth_cache_path: If specified, will cache auth data here
        :param prefix: Prefix path for DB if needed
        '''
        self.__bucket_name = bucket
        self.__prefix = prefix

        if b2_auth_cache_path:
            b2_auth_data = SqliteAccountInfo(b2_auth_cache_path)
        else:
            b2_auth_data = InMemoryAccountInfo()

        self.__b2 = B2Api(b2_auth_data)

        self.__b2.authorize_account("production", b2_key_id, b2_secret)
        self.__bucket = self.__b2.get_bucket_by_name(self.__bucket_name)

        self.__cache = None


    @property
    def b2(self):
        return self.__b2


    @property
    def bucket(self):
        return self.__bucket


    def __getattribute__(self, name):
        '''
        Wrap models attached to this Database class as Tables

        Derived classes are expected to create class attributes that represent
        tables in the database and point to custom Model classes.

        Example:

            class MyModel(b2db.Model):
                name = b2db.CharField()

            class MyDatabase(b2db.Database):
                my_records = MyModel

        This method wraps these model references with b2db.Table classes to provide
        access to the methods to interact with those tables.

        Python NOTES:
            __getattribute__() called on every attribute access
            To access attribute without calling this method:
                object.__getattribute__(self, name)
        '''
        obj = object.__getattribute__(self, name)

        # Wrap Model attributes as Table objects
        try:
            if not name.startswith('_') and name.lower() == name:
                try:
                    if not obj.is_b2db_model:
                        raise NotAModelAttribute()
                except AttributeError:
                    raise NotAModelAttribute()
                return Table(db = self, model_attr_name=name, model=obj)
        except NotAModelAttribute:
            pass

        # Return attribute without wrapping into a table
        return obj


    def nuke(self):
        '''
        Delete all contents of the database
        '''
        for file_info, folder_name in self.__bucket.ls(show_versions=False):
            self.__bucket.hide_file(file_info.file_name)
        self.clear_cache()


    def clear_cache(self):
        '''Empty out all the cached values'''
        if self.__cache:
            self.__cache.clear()


    def write_record_data(self, path, save_data):
        '''
        Write record data to a file in the storage

        :param path: Path to write the record data to in the cloud storage
        :param save_data: dictionary of data to encode and write
        '''

        # Encode data
        try:
            encoded_data = json.dumps(save_data)
        except Exception as e:
            raise ValueError("Failed to encode record data to JSON: %s: %s: %s" % (
                repr(save_data), e.__class__.__name__, str(e)))

        # Convert to bytes
        try:
            encoded_data = encoded_data.encode('utf-8')
        except Exception as e:
            raise ValueError("Failed to encode record data to UTF-8: %s: %s: %s" % (
                repr(encoded_data), e.__class__.__name__, str(e)))

        # Apply prefix
        if self.__prefix:
            path = self.__prefix + '/' + path

        # Save data
        r = self.__bucket.upload_bytes(encoded_data, path,
                                       content_type='application/json')

        print("TODO: Cache with version")
        # # Cache record
        # if self.__cache:
        #     self.__cache[path] = save_data.copy()


    def delete_record_files(self, path):
        '''
        Delete a record key from a table

        :param path: Path to folder that holds record's files
        '''
        cnt = 0

        # Apply prefix
        if self.__prefix:
            path = self.__prefix + '/' + path

        # List files under path
        for file, folder in self.bucket.ls(path, show_versions=False, recursive=True):

            # Hide files so they don't appear in list by name
            self.__bucket.hide_file(file.file_name)
            cnt += 1

        if cnt == 0:
            raise KeyError("No files to delete at " + path)

        print("TODO: Update cache after delete")
        # # Cache record
        # if self.__cache:
        #     self.__cache[path] = save_data.copy()


    def read_record_data(self, path):
        '''
        Read record data back from file in the storage

        :param path: Path to write the record data to in the cloud storage
        :param save_data: dictionary of data to encode and write
        '''

        # Apply prefix
        if self.__prefix:
            path = self.__prefix + '/' + path

        # Get current file ID
        print("Cache current File ID?")
        if False:
            # ref: https://b2-sdk-python.readthedocs.io/en/master/quick_start.html#by-id
            pass
            # file_info = None
            # for b2obj in self.__bucket.ls(path):
            #     file_info = b2obj
            # if not file_info:
            #     raise KeyError("Record %s not in table" % (os.path.basename(path)))

        # Else, retireve by filename
        download_buffer = DownloadDestBytes()
        try:
            self.__bucket.download_file_by_name(path, download_buffer)
        except FileNotPresent:
            raise KeyError("No record saved at " + path)

        # Decode bytes
        try:
            decoded_data = download_buffer.get_bytes_written().decode('utf-8')
        except Exception as e:
            raise ValueError("Failed to decode record data %s from UTF-8: %s: %s" % (
                path, e.__class__.__name__, str(e)))

        # Decode JSON
        try:
            decoded_data = json.loads(decoded_data)
        except Exception as e:
            raise ValueError("Failed to decode record data in %s from JSON: %s: %s" % (
                path, e.__class__.__name__, str(e)))

        print("TODO: Cache read data")

        return decoded_data


    RECORD_DATA_PAT = re.compile(r'^(.*)/record-data\.json$')

    def list_table_keys(self, path):
        '''
        List all keys for a table

        :param path: Path to table files relative to db root
        :return: Generate of keys that can be retrieved for this table
        '''

        # Apply prefix
        if self.__prefix:
            path = self.__prefix + '/' + path

        # List all files in table
        for file, folder in self.__bucket.ls(path, show_versions=False, recursive=True):

            # Make path relative to table root
            file_path = file.file_name
            if not file_path.startswith(path+'/'):
                raise Exception("Got path '%s', that doesn't start with '%s'" % (
                    file_path, path+'/'))
            file_path = file_path[len(path)+1:]

            # See if this is a Record data file
            m = self.RECORD_DATA_PAT.match(file_path)
            if m:
                yield m.group(1)
