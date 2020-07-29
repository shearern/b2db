import os
from mimetypes import guess_type

import filetype


def normalize_b2_path(key):
    '''Change key so that it doesn't interfere with cloud storage structure'''
    return key.replace("\\", '/')


def determine_file_type(path):

    # Try Python magic
    kind = filetype.guess(path)
    if kind is not None:
        return kind.mime

    return guess_type(os.path.basename(path))[0]



class ModelFileHandle:
    '''
    Used to provide functionality to interact with files attached to records

    value indicates file object data from B2 that was uploaded. Example:
    {
        'fileId': '4_z9054c0781f4f079173300413_f114d725162f9816f_d20200723_m063529_c002_v0001125_t0049',
        'fileName': 'unittests/test-0/test-file',
        'size': 4,
        'uploadTimestamp': 1595486129000,
        'action': 'upload',
    }
    '''

    def __init__(self, record, attr_name, value, attr_options):
        '''
        :param record: The record we're accessing files for
        :param attr_name: The name of the attribute this file is set for
        :param value: If file has been uploaded, then B2 object data
        :param attr_options: FileField object with options specified in model
        '''
        self.__record = record
        self.__attr_name = attr_name
        self.__value = value
        self.__options = attr_options


    @property
    def size(self):
        if self.__value:
            return int(self.__value['size'])


    def exists(self):
        '''Has a file been uploaded for this attribute'''
        return self.__value is not None


    def upload(self, path):
        '''Upload a local file into the DB for this attribute'''

        target_path = os.path.basename(path)
        if self.__options.set_filename:
            target_path = self.__options.set_filename
        if self.__options.upload_to:
            target_path = '/'.join((self.__options.upload_to, target_path))

        content_type = determine_file_type(path)
        if self.__options.accept_content_types is not None:
            if content_type not in self.__options.accept_content_types:
                raise ValueError("File has content type '%s', which is not in (%s)" % (
                    content_type, ', '.join(self.__options.accept_content_types)))
        if self.__options.set_content_type:
            content_type = content_type

        b2obj = self.__record._table._upload_record_file(
            key = self.__record.key,
            content_type = content_type,
            path = path,
            target = target_path)

        file_info = b2obj.as_dict()
        file_info['saved_name'] = target_path
        self.__record[self.__attr_name] = file_info


    def download(self, path):
        '''Download file to disk'''
        if self.__value is None:
            raise ValueError("Can't download file, not set yet.")

        if self.__options.use_version:
            self.__record._table._download_record_file_by_id(
                key = self.__record.key,
                file_id = self.__value['fileId'],
                path = path)
        else:
            self.__record._table._download_record_file_by_name(
                key = self.__record.key,
                name = self.__value['saved_name'],
                path = path)


