import json


class NotAFieldTypeAttribute(Exception): pass


class Model(object):
    '''
    Defines the structure of a table in the B2Database

    Intended to be subclassed to define the tables in a database.

    Class represents the structure of a table in the DB.
    Instance represents a specific record in the table
    '''

    is_b2db_model = True

    def __init__(self, table, key, saved_data=None):
        self.__key = key
        self.__table = table
        self.__data = dict()
        self.__data = saved_data or dict()
        self.__changed = False


    def __str__(self):
        return self.__key


    @property
    def key(self):
        return self.__key


    @property
    def _table(self):
        return self.__table


    def __getitem__(self, key):
        '''Access the raw data (decoded JSON) for a given attribute.  None if doesn't exist'''
        try:
            return self.__data[key]
        except KeyError:
            return None


    def __setitem__(self, key, value):
        '''Sets the raw data stored in the record'''
        if value.__class__ is None:
            if key in self.__data:
                del self.__data[key]
        elif value.__class__ not in (str, int, float, list, dict):
            raise ValueError("Type %s probably not supported by JSON.  Did you mean %s.%s?" % (
                self.__class__.__name__, key))
        self.__data[key] = value
        self.__changed = True


    def __setattr__(self, name, value):
        '''
        Provide access to field type attributes as setting the value of the field

        Derived classes are expected to create class attributes that represent
        fields in the database and point to the field_types.* helpers for
        formatting the data.

        Example:

            class MyModel(b2db.Model):
                name = b2db.CharField()

            rec.name = 'John'

        Python NOTES:
            1. __setattr__() called on every attribute set (not just missing attrs)
            2. To set attribute without calling this method:
                  object.__setattr__(self, name, value)
            3. setattr() seems to bypass this method.  Call this.__setattr__() instead
        '''

        try:

            # Get attribute to see if it's a field type
            if name.startswith('_') or name.lower() != name:
                raise  NotAFieldTypeAttribute()
            try:
                obj = object.__getattribute__(self, name)
                if not obj.is_b2db_field_type:
                    raise NotAFieldTypeAttribute()
            except AttributeError:
                raise NotAFieldTypeAttribute()

            # Use Field Type to encode value
            field_type = obj
            if not field_type.is_settable:
                raise AttributeError("Can't set field type %s" % (field_type.__class__.__name__))
            self[name] = field_type.format(record=self, attr_name=name, value=value)

        # Standard attribute.  Let object do it's thing
        except NotAFieldTypeAttribute:
            object.__setattr__(self, name, value)


    def __getattribute__(self, name):
        '''
        Return decoded field values

        set __setattr__()

        Python NOTES:
            __getattribute__() called on every attribute access
            To access attribute without calling this method:
                object.__getattribute__(self, name)
        '''
        obj = object.__getattribute__(self, name)
        try:
            # Get attribute and see if it's a field type
            if name.startswith('_') or name.lower() != name:
                raise  NotAFieldTypeAttribute()
            try:
                if not obj.is_b2db_field_type:
                    raise NotAFieldTypeAttribute()
            except AttributeError:
                raise NotAFieldTypeAttribute()

            # Use Field Type to decode value
            field_type = obj
            return field_type.parse(record=self, attr_name=name, value=self[name])

        # Not a field attribute.  Return without any extra actions
        except NotAFieldTypeAttribute:
            return obj


    def save(self, force=False):
        '''Write this record to storage'''
        if self.__changed or force:
            self.__table.write_record_data(self.__key, self.__data)
            self.__changed = False


    def delete(self):
        '''Delete this record from the table'''
        self.__table.delete(self.__key)


