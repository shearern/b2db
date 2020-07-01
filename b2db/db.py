
from b2sdk.v1 import B2Api
from b2sdk.v1 import InMemoryAccountInfo, SqliteAccountInfo


class B2Database:

    def __init__(self, bucket, b2_key_id, b2_secret, b2_auth_cache_path, prefix=None):
        '''
        :param bucket: Bucket name to use
        :param b2_key_id: B2 key to authenticate with
        :param b2_secret: B2 secret to authenticate with
        :param b2_auth_cache_path: If specified, will cache auth data here
        :param prefix: Prefix path for DB if needed
        '''
        self.__bucket = bucket
        self.__prefix = prefix

        if b2_auth_cache_path:
            b2_auth_data = SqliteAccountInfo(b2_auth_cache_path)
        else:
            b2_auth_data = InMemoryAccountInfo()

        self.__b2 = B2Api(b2_auth_data)

        self.__b2.authorize_account("production", b2_key_id, b2_secret)

        self._create_table_attributes()


    def _create_table_attributes(self):
        '''Used by derived classes to create table access attributes'''
        pass
