import os
import shutil
import requests
import exceptions

import dbutils
import cache

import logging
import local_config
logger = logging.getLogger(__name__)
local_config.configure_logger(logger)



class BinaryCache:
    '''
    Utility class to manage file caching properly.
    '''

    def __init__(self,
                 name, device_config_path, remote_url, device_mount_path):
        '''
        Register information required to handle caching.
        '''
        self.name = name
        self.device_config_path = device_config_path
        self.remote_url = remote_url
        self.device_mount_path = device_mount_path

        self.cache_path = os.path.join(device_config_path, 'cache')
        self.db = dbutils.get_db(self.name)
        self.metadata_cache = cache.Cache()

        if not os.path.isdir(self.cache_path):
            os.makedirs(self.cache_path)

    def get_file_metadata(self, path):
        '''
        Returns file metadata based on given path. The corresponding file doc is
        returned with the linked binary ID and the cached file path.
        '''
        res = self.metadata_cache.get(path)
        if res is None:
            file_doc = dbutils.get_file(self.db, path)
            binary = file_doc["binary"]
            binary_id = binary["file"]["id"]
            cache_file_folder = os.path.join(self.cache_path, binary_id)
            cache_file_name = os.path.join(cache_file_folder, 'file')

            res = (file_doc, binary_id, cache_file_name)
            self.metadata_cache.add(path, res)
        return res

    def is_cached(self, path):
        '''
        Returns True is the file is already present in the cache folder.
        '''
        (file_doc, binary_id, filename) = self.get_file_metadata(path)

        return os.path.exists(filename)

    def get(self, path, mode='r'):
        '''
        Returns the required file from the cache (local file system).
        '''
        (file_doc, binary_id, filename) = self.get_file_metadata(path)

        return open(filename, mode)

    def add(self, path, data=None):
        '''
        If no data is given, it downloads the binary from configured CouchDB
        and save it in the cache folder. File is marked as stored in the file
        metadata.
        If data is given, it creates a new binary with that data but don't
        upload anything in CouchDB.
        '''
        (file_doc, binary_id, filename) = self.get_file_metadata(path)
        cache_file_folder = os.path.join(self.cache_path, binary_id)
        logger.info('binay_cache.add: %s %s' % (path, filename))

        # Create cache folder for given binary
        if not os.path.isdir(cache_file_folder):
            os.mkdir(cache_file_folder)

        # Create file.
        if data is not None:
            with open(filename, 'wb') as fd:
                fd.write(data)
        else:
            url = '%s/%s/%s' % (self.remote_url, binary_id, 'file')
            req = requests.get(url, stream=True)
            if req.status_code != 200:
                raise exceptions.IOError(
                    "File not stored in the local CouchDB database %s" % url)
            else:
                with open(filename, 'wb') as fd:
                    for chunk in req.iter_content(1024):
                        fd.write(chunk)

            # Update metadata.
            file_doc['size'] = os.path.getsize(filename)
            self.mark_file_as_stored(file_doc)

    def update_size(self, path):
        '''
        Get size of current cached binary and update file size metadata with
        information from the binary.
        '''
        (file_doc, binary_id, filename) = self.get_file_metadata(path)
        logger.info('update_size: %s' % path)
        file_doc['size'] = os.path.getsize(filename)
        dbutils.update_file(self.db, file_doc)
        self.metadata_cache.add(path, (file_doc, binary_id, filename))
        return file_doc['size']

    def update(self, path, data, mode='ab'):
        '''
        Write on the cached binary of file located at path in the virtual file
        system. Offset is where the writing should start.
        '''
        logger.info('binary_cache.update: %s' % path)
        with self.get(path, mode) as binary:
            logger.info('binary_cache.update: %s' % binary)
            binary.write(data)

    def remove(self, path):
        '''
        Remove file from cache. Mark file as not stored in the database.
        '''
        (file_doc, binary_id, filename) = self.get_file_metadata(path)

        cache_file_folder = os.path.join(self.cache_path, binary_id)
        if os.path.exists(cache_file_folder):
            shutil.rmtree(cache_file_folder)
        self.metadata_cache.remove(path)
        self.mark_file_as_not_stored(file_doc)

    def mark_file_as_stored(self, file_doc):
        '''
        Mark file as stored in the database. It's done by adding the device
        name to the storage list field.
        '''
        if file_doc.get('storage', None) is None:
            file_doc['storage'] = [self.name]
        elif not (self.name in file_doc['storage']):
            file_doc['storage'].append(self.name)
        dbutils.update_file(self.db, file_doc)

    def mark_file_as_not_stored(self, file_doc):
        '''
        Remove the device name from the storage list linked to the given
        file_doc.
        '''
        if file_doc.get('storage', None) is None:
            return
        elif self.name in file_doc['storage']:
            file_doc['storage'].remove(self.name)

        dbutils.update_file(self.db, file_doc)
