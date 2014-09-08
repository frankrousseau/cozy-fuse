#!/usr/bin/env python

# -*- coding: utf-8 -*-
#
# Copyright (C) 2008 Jason Davies
# All rights reserved.
#
# This software is licensed as described in the file COPYING, which
# you should have received as part of this distribution.

import os
import platform
import errno
import fuse
import stat
import subprocess
import logging
import datetime
import ntpath
import mimetypes
import re

import cache
import fusepath
import dbutils
import binarycache
import local_config

from couchdb import ResourceNotFound

ATTR_VALIDITY_PERIOD = datetime.timedelta(seconds=10)

DEVNULL = open(os.devnull, 'wb')
EXCLUDED_PATTERNS = ['^\.(.*)', '(.*)~$']

fuse.fuse_python_api = (0, 2)

CONFIG_FOLDER = os.path.join(os.path.expanduser('~'), '.cozyfuse')
HDLR = logging.FileHandler(os.path.join(CONFIG_FOLDER, 'cozyfuse.log'))
HDLR.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s'))

logger = logging.getLogger(__name__)
logger.addHandler(HDLR)
logger.setLevel(logging.INFO)



class CouchStat(fuse.Stat):
    '''
    Default file descriptor.
    '''

    def __init__(self):
        self.st_mode = 0
        self.st_ino = 0
        self.st_dev = 0
        self.st_nlink = 0
        self.st_uid = os.getuid()
        self.st_gid = os.getgid()
        self.st_size = 4096
        self.st_atime = 0
        self.st_mtime = 0
        self.st_ctime = 0
        self.st_blocks = 0

    def set_root(self):
        '''
        Set attributes to match the root folder of the current FS.
        '''
        self.st_mode = stat.S_IFDIR | 0o775
        self.st_nlink = 2

    def set_folder(self, folder):
        '''
        Set attributes to match folder ones.
        '''
        self.st_mode = stat.S_IFDIR | 0o775
        self.st_nlink = 2
        if 'lastModification' in folder:
            self.st_atime = fusepath.get_date(folder['lastModification'])
            self.st_ctime = self.st_atime
            self.st_mtime = self.st_atime

    def set_file(self, file_doc):
        '''
        Set attributes to match file ones.
        '''
        self.st_mode = stat.S_IFREG | 0o664
        self.st_nlink = 1
        self.st_size = file_doc.get('size', 4096)
        if 'lastModification' in file_doc:
            self.st_atime = fusepath.get_date(file_doc['lastModification'])
            self.st_ctime = self.st_atime
            self.st_mtime = self.st_atime


class CouchFSDocument(fuse.Fuse):
    '''
    Fuse implementation behavior: handles synchronisation with device when a
    change occurs or when users want to access to his/her file system.
    '''

    def __init__(self, device_name, mountpoint, uri=None, *args, **kwargs):
        '''
        Configure file system, device and store remote Cozy informations.
        '''
        logger.info('Configuring CouchDB Fuse...')

        # Configure fuse
        fuse.Fuse.__init__(self, *args, **kwargs)
        self.fuse_args.mountpoint = mountpoint
        self.fuse_args.add('allow_other')
        self.currentFile = None
        logger.info('- Fuse configured')

        # Configure device
        self.device = device_name
        (self.db, self.server) = dbutils.get_db_and_server(device_name)
        logger.info('- Database configured')

        # Configure Cozy
        device = dbutils.get_device(device_name)
        self.urlCozy = device.get('url', '')
        self.passwordCozy = device.get('password', '')
        self.loginCozy = device_name
        logger.info('- Cozy configured')

        # Configure replication urls.
        (self.db_username, self.db_password) = \
            local_config.get_db_credentials(device_name)
        self.rep_source = 'http://%s:%s@localhost:5984/%s' % (
            self.db_username,
            self.db_password,
            self.device
        )
        self.rep_target = "https://%s:%s@%s/cozy" % (
            self.loginCozy,
            self.passwordCozy,
            self.urlCozy.split('/')[2]
        )
        logger.info('- Replication configured')

        # Configure cache and create required folders
        device_path = os.path.join(CONFIG_FOLDER, device_name)
        self.binary_cache = binarycache.BinaryCache(
            device_name, device_path, self.rep_source, mountpoint)
        self.file_size_cache = cache.Cache()
        self.attr_cache = cache.Cache()
        self.name_cache = cache.Cache()
        self.fd_cache = cache.Cache()

        logger.info('- Cache configured')

    def getattr(self, path):
        """
        Return file descriptor for given_path. FS requires constantly
        information about file attributes. It's important to make this method
        very fast.

        Useful for 'ls -la' command like.
        """
        try:
            logger.info('getattr %s' % path)
            path = fusepath.normalize_path(path)

            # Try to get attribute from local cache.
            attr = self.attr_cache.get(path)
            if attr is not None:
                return attr

            else:

                # It's the root folder.
                if path == "/" or path == '':
                    st = CouchStat()
                    st.set_root()

                else:
                    # Avoid to check in database if non existing file/folder
                    # exists.
                    if not self._is_in_list_cache(path):
                        logger.info('Not found (not in list cache): %s' % path)
                        return -errno.ENOENT
                    else:
                        # Build attributes from database metadata.
                        st = self._get_attr_from_db(path)

                # If no st was built, the file is considered as absent.
                if st is None:
                    logger.info('Not found (not in database): %s' % path)
                    return -errno.ENOENT
                else:
                    return st

        except Exception as e:
            logger.exception(e)
            return -errno.ENOENT

    def mkdir(self, path, mode):
        """
        Create folder in current FS add a folder in the database and update
        cache accordingly.
            path {string}: diretory path
            mode {string}: directory permissions
        """
        logger.info('mkdir %s' % path)
        try:
            path = fusepath.normalize_path(path)
            parent_path, name = fusepath.split(path)

            now = fusepath.get_current_date()
            folder = dbutils.get_folder(self.db, path)

            # Check folder existence.
            if folder is not None:
                logger.info('folder already exists %s' % path)
                return -errno.EEXIST

            # Create folder.
            else:
                logger.info('folder creation... %s %s' % (parent_path, name))
                folder = dbutils.create_folder(self.db, {
                    "name": name,
                    "path": parent_path,
                    "docType": "Folder",
                    'creationDate': now,
                    'lastModification': now,
                })

                self._update_parent_folder(parent_path)
                self._add_to_cache(path)

                return 0

        except Exception as e:
            logger.exception(e)
            return -errno.EEXIST

    def mknod(self, path, mode, dev):
        """
        Create a new node on the CouchFS. It leads to prepare file creation by
        creating a binary document and a file document in the database.
        Then it creates binary cache file (empty file).

        Parent folder last modification date is updated.
        """
        try:
            logger.info('mknod %s, %s, %s' % (dev, mode, path))
            path = fusepath.normalize_path(path)

            binary_id = self._create_empty_binary_in_db()
            self._create_new_file_in_db(path, binary_id)
            self._create_new_file(path)
            self._update_parent_folder(path)
            logger.info('mknod is done for %s' % path)
            return 0

        except Exception as e:
            logger.exception(e)
            return -errno.ENOENT


    def open(self, path, flags):
        """
        Open file, mainly check if the file exists or not.
            path {string}: file path
            flags {string}: opening mode
        """
        try:
            logger.info('open %s, %s' % (flags, path))
            path = fusepath.normalize_path(path)
            if self._is_found(path):
                if (flags & 3) == os.O_RDONLY or (flags & 3) == os.O_RDWR:
                    if not self.binary_cache.is_cached(path):
                        self.binary_cache.add(path)

                    (file_doc, binary_id, filename) = self.binary_cache.get_file_metadata(path)
                    fd = os.open(filename, flags)
                    self.fd_cache.add(path, fd)
                    return 0
                elif (flags & 3) == os.O_WRONLY:
                    if not self.binary_cache.is_cached(path):
                        self.binary_cache.add(path, '')
                    (file_doc, binary_id, filename) = self.binary_cache.get_file_metadata(path)
                    fd = os.open(filename, flags)
                    self.fd_cache.add(path, fd)
                    return 0

                else:
                    logger.info('no write, noread')
                    return -errno.EINVAL
            else:
                logger.error('File not found %s' % path)
                return -errno.ENOENT
        except Exception as e:
            logger.exception(e)
            return -errno.ENOENT

    def read(self, path, length, offset):
        """
        Return content of binary cache of file located at given path.
        It extracts binary from remote Cozy and save it in a cache folder if
        it does not already exists.
            path {string}: file path
            size {integer}: size of file part to read
            offset {integer}=: beginning of file part to read
        """
        try:
            logger.info('read %s' % path)
            path = fusepath.normalize_path(path)
            fh = self.fd_cache.get(path)
            logger.info(fh)
            os.lseek(fh, offset, os.SEEK_SET)
            return os.read(fh, length)
        except Exception as e:
            logger.exception(e)
            return -errno.ENOENT

    def readdir(self, path, offset):
        """
        Generator: list files for given path and yield each file result when
        it arrives.
        Perform doc and attr caching for each returned results.
        """
        logger.info('readdir %d %s' % (offset, path))
        path = fusepath.normalize_path(path)

        names = ['.', '..'] + self._get_names(path)
        for name in names:
            yield fuse.Direntry(name.encode('utf-_8'))

    def release(self, path, flags):
        """
        It's the method called after writing operations are ended.
        Ii saves file size metadata to database.
        """
        try:
            logger.info('release %s' % path)
            path = fusepath.normalize_path(path)

            fd = self.fd_cache.get(path)
            os.close(fd)
            if (flags & 3) == os.O_WRONLY:
                try:
                    size = self.binary_cache.update_size(path)
                    logger.info('step 1')
                    self.file_size_cache.add(path, size)
                    logger.info('step 2')
                    self._get_attr_from_db(path, isfile=True)
                    logger.info('step 3')
                    self._add_to_cache(path)
                    logger.info('file released')
                except ResourceNotFound:
                    logger.info('release error file not found')
                    self._clean_cache(path)
                return 0
            else:
                logger.info('No file descriptor')
                return -errno.ENOENT

        except Exception as e:
            logger.exception(e)
            return -errno.ENOENT

    def rename(self, pathfrom, pathto, root=True):
        """
        Rename file and subfiles (if it's a folder) in device.
        """
        logger.info("rename %s -> %s: " % (pathfrom, pathto))
        try:
            pathfrom = fusepath.normalize_path(pathfrom)
            pathto = fusepath.normalize_path(pathto)

            file_doc = dbutils.get_file(self.db, pathfrom)
            if file_doc is not None:
                file_path, name = fusepath.split(pathto)

                file_doc.update({
                    "name": name,
                    "path": file_path,
                    "lastModification": fusepath.get_current_date()
                })
                dbutils.update_file(self.db, file_doc)

            folder_doc = dbutils.get_folder(self.db, pathfrom)
            if folder_doc is not None:
                folder_path, name = fusepath.split(pathto)
                folder_doc.update({
                    "name": name,
                    "path": folder_path,
                    "lastModification": fusepath.get_current_date()
                })

                # Rename all subfiles
                for res in self.db.view("file/byFolder", key=pathfrom):
                    child_pathfrom = os.path.join(
                        res.value['path'],
                        res.value['name']
                    )
                    child_pathto = os.path.join(folder_path, name, res.value['name'])
                    self.rename(child_pathfrom, child_pathto, False)

                for res in self.db.view("folder/byFolder", key=pathfrom):
                    child_pathfrom = os.path.join(
                        res.value['path'],
                        res.value['name'])
                    child_pathto = os.path.join(folder_path, name, res.value['name'])
                    self.rename(child_pathfrom, child_pathto, False)

                dbutils.update_folder(self.db, folder_doc)

            parent_path_from, namefrom = fusepath.split(pathfrom)
            parent_path_to, nameto = fusepath.split(pathto)

            if root:
                self._update_parent_folder(parent_path_from)
                self._update_parent_folder(parent_path_to)

            names = dbutils.name_cache.get(parent_path_from)
            if names is not None and namefrom in names:
                names.remove(namefrom)
                names.add(parent_path_from, names)

            names = dbutils.name_cache.get(parent_path_to)
            if names is not None:
                names.append(nameto)
                names.add(parent_path_to, names)

            self._clean_cache(pathfrom)
            self._add_to_cache(pathto)

            if folder_doc is None and file_doc is None:
                return -errno.ENOENT
            else:
                return 0

        except Exception as e:
            logger.exception(e)
            return -errno.ENOENT

    def rmdir(self, path):
        """
        Delete folder from database and clean caches accordingly.
            path {string}: folder path
        """
        logger.info('rmdir %s' % path)
        try:
            path = fusepath.normalize_path(path)
            folder = dbutils.get_folder(self.db, path)
            dbutils.delete_folder(self.db, folder)
            self._clean_cache(path)
            return 0

        except Exception as e:
            logger.exception(e)
            return -errno.ENOENT

    def unlink(self, path):
        """
        Remove file from current FS. Update cache accordingly.
        """
        try:
            logger.info('unlink %s' % path)
            path = fusepath.normalize_path(path)

            if dbutils.get_file(self.db, path) is not None:
                self.binary_cache.remove(path)
                self._clean_cache(path, True)
                self._remove_file_from_db(path)
                self._update_parent_folder(path)
                return 0
            else:
                logger.info('Cannot delete file, no entry found')
                return -errno.ENOENT

        except Exception as e:
            logger.exception(e)
            return -errno.ENOENT

    def write(self, path, buf, offset):
        """
        Write data in binary cache of file located at given path.
            path {string}: file path
            buf {buffer}: data to write
        """
        logger.info('write %s: %s' % (offset, path))
        path = fusepath.normalize_path(path)
        fh = self.fd_cache.get(path)
        os.lseek(fh, offset, os.SEEK_SET)
        val = os.write(fh, buf)
        logger.info(val)
        attr = self.attr_cache.get(path)
        logger.info(attr)
        attr.st_size = self.binary_cache.get_current_size(path)
        logger.info(attr)
        self.attr_cache.add(path, attr)
        return val

    def fsync(self, path, isfsyncfile):
        logger.info('fsync %s, %s' % (path, isfsyncfile))
        return 0

    def access(self, path, mode):
        logger.info('access %s, %s' % (path, mode))
        #return 0

    def chmod(self, path, mode):
        logger.info('chmod %s, %s' % (path, mode))
        return 0

    def chown(self, path, uid, gid):
        logger.info('chown %s, %s, %s' % (path, uid, gid))
        return 0

    #def symlink(self, target, name):
        #logger.info('symlink %s, %s' % (target, name))
        #return 0

    #def link(self, target, name):
        #logger.info('link %s, %s' % (target, name))
        #return 0

    #def utime(self, path, times):
        #logger.info('utime %s, %s' % (path, times))
        #return 0

    #def utimens(self, path, times=None):
        #logger.info('utimens %s, %s' % (path, times))
        #return 0

    def truncate(self, path, length, fh=None):
        logger.info('truncate %s, %s' % (path, length))
        return 0

    #def flush(self, path, fh):
        #logger.info('flush %s, %s' % (path, fh))
        #return 0

    def statfs(self):
        """
        It is the file system global attributes.

        Should return a tuple with the following 6 elements:
            - blocksize - size of file blocks, in bytes
            - totalblocks - total number of blocks in the filesystem
            - freeblocks - number of free blocks
            - availblocks - number of blocks available to non-superuser
            - totalfiles - total number of file inodes
            - freefiles - nunber of free file inodes

        Feel free to set any of the above values to 0, which tells
        the kernel that the info is not available.
        """
        disk_space = dbutils.get_disk_space(
            self.device,
            self.urlCozy,
            self.loginCozy,
            self.passwordCozy)
        st = fuse.StatVfs()

        blocks = float(disk_space['totalDiskSpace']) * 1000 * 1000
        block_size = 1000
        blocks_free = float(disk_space['freeDiskSpace']) * 1000 * 1000
        blocks_avail = blocks_free

        files = 0
        files_free = 0

        st.f_bsize = block_size
        st.f_frsize = block_size
        st.f_blocks = blocks
        st.f_bfree = blocks_free
        st.f_bavail = blocks_avail
        st.f_files = files
        st.f_ffree = files_free

        return st

    def _is_found(self, path):
        '''
        Returns true if the path exists in the database, false either.
        '''
        path = fusepath.normalize_path(path)
        file_doc = dbutils.get_file(self.db, path)
        return file_doc is not None

    def _create_new_file(self, path):
        '''
        Create empty binary cache and load file metadata from database.
        '''
        file_doc = dbutils.get_file(self.db, path)
        file_doc['size'] = 0
        file_doc['lastModification'] = fusepath.get_current_date()
        dbutils.update_file(self.db, file_doc)

    def _create_empty_binary_in_db(self):
        '''
        Create an empty binary object in database and returns its id.
        '''
        new_binary = {"docType": "Binary"}
        binary_id = self.db.create(new_binary)
        self.db.put_attachment(self.db[binary_id], '', filename="file")
        return binary_id

    def _create_new_file_in_db(self, path, binary_id):
        '''
        Create new file document (metadata) in database. Set link with given
        binary id. Then update caches accordingly.
        '''
        file_path, name = ntpath.split(path)
        file_path = fusepath.normalize_path(file_path)
        (mime_type, encoding) = mimetypes.guess_type(path)
        rev = self.db[binary_id]["_rev"]
        now = fusepath.get_current_date()
        newFile = {
            "name": name.decode('utf8'),
            "path": fusepath.normalize_path(file_path).decode('utf8'),
            "binary": {
                "file": {
                    "id": binary_id,
                    "rev": rev
                }
            },
            "docType": "File",
            "mime": mime_type,
            'creationDate': now,
            'lastModification': now,
        }
        dbutils.create_file(self.db, newFile)
        names = self.name_cache.get(file_path)
        if names is not None:
            names.append(name)

    def _remove_file_from_db(self, path):
        '''
        Remove binary document if it exists, then remove file document.
        '''
        file_doc = dbutils.get_file(self.db, path)
        if file_doc["binary"] is not None and 'file' in file_doc["binary"]:
            binary_id = file_doc["binary"]["file"]["id"]
            try:
                self.db.delete(self.db[binary_id])
            except ResourceNotFound:
                pass
        dbutils.delete_file(self.db, file_doc)

    def _update_parent_folder(self, parent_folder):
        """
        Update parent folder
            parent_folder {string}: parent folder path

        When a file or a folder is renamed/created/removed, last modification
        date of parent folder should be updated

        """
        folder = dbutils.get_folder(self.db, parent_folder)
        if folder is not None:
            folder['lastModification'] = fusepath.get_current_date()
            dbutils.update_folder(self.db, folder)

    def _add_to_cache(self, path, isfile=False):
        dirname, name = fusepath.split(path)
        names = self.name_cache.get(dirname)
        if names is not None and not name in names:
            names.append(name)

    def _clean_cache(self, path, isfile=False):
        '''
        Remove ref of given path from all caches.
        '''
        #logger.info('clean cache: %s' % path)
        self.attr_cache.remove(path)

        dirname, name = ntpath.split(path)
        dirname = fusepath.normalize_path(dirname)
        names = self.name_cache.get(dirname)
        if names is not None and name in names:
            names.remove(name)

        if isfile:
            self.binary_cache.remove(path)
            self.file_size_cache.remove(path)
            dbutils.file_cache.remove(path)
        else:
            dbutils.folder_cache.remove(path)

    def _get_names(self, path):
        '''
        Return name of files and folders located at folder path. It put every
        return results in cache to fasten coming requests. Fuse runs a lot of
        getattr after getting name list (requested via a readdir call).

        Dirtly written to avoid running through folders and files too much
        time.
        '''
        names = self.name_cache.get(path)
        if names is None:
            names = []

            res = self.db.view('file/byFolder', key=path)
            for doc in res:
                name = doc.value["name"]
                names.append(name)
                filepath = os.path.join(path.encode('utf-8'), name.encode('utf-8'))
                filepath = fusepath.normalize_path(filepath)
                dbutils.file_cache.add(filepath, doc.value)
                self._get_attr_from_db(filepath, isfile=True)

            res = self.db.view('folder/byFolder', key=path)
            for doc in res:
                name = doc.value["name"]
                names.append(name)
                folderpath = os.path.join(path.encode('utf-8'), name.encode('utf-8'))
                folderpath = fusepath.normalize_path(folderpath)
                dbutils.folder_cache.add(folderpath, doc.value)
                self._get_attr_from_db(folderpath, isfile=False)

            names.sort()
            self.name_cache.add(path, names)

        return names

    def _is_in_list_cache(self, path):
        '''
        When folder content list is request, all folder and file names are
        cached. This returns true if given path is listed in that list.
        '''
        dirname, filename = ntpath.split(path)
        dirname = fusepath.normalize_path(dirname)
        names = self._get_names(dirname)
        if not filename.decode('utf-8') in names:
            logger.info('File does not exist in cache: %s' % path)
            return False
        else:
            return True

    def _get_attr_from_db(self, path, isfile=None):
        '''
        Build fuse file attribute from data located in database. Check if path
        corresponds to a folder first.
        '''
        st = CouchStat()
        path = fusepath.normalize_path(path)

        if isfile is None:
            folder = dbutils.get_folder(self.db, path)
            if folder is not None:
                st.set_folder(folder)
                self.attr_cache.add(path, st)
                return st
            else:
                file_doc = dbutils.get_file(self.db, path)
                if file_doc is not None:
                    st.set_file(file_doc)
                    self.attr_cache.add(path, st)
                    return st
                else:
                    return None
        elif isfile:
            file_doc = dbutils.get_file(self.db, path)
            if file_doc is not None:
                st.set_file(file_doc)
                self.attr_cache.add(path, st)
                return st
            else:
                return None
        else:
            folder = dbutils.get_folder(self.db, path)
            if folder is not None:
                st.set_folder(folder)
                self.attr_cache.add(path, st)
                return st
            else:
                return None


def unmount(path):
    '''
    Unmount folder given Fuse folder.
    '''
    if platform.system() == "Darwin" or platform.system() == "FreeBSD":
        command = ["umount", path]
    else:
        command = ["fusermount", "-u", path]

    # Do not display fail messages at unmounting
    subprocess.call(command, stdout=DEVNULL, stderr=subprocess.STDOUT)
    logger.info('Folder %s unmounted' % path)


def mount(name, path):
    '''
    Mount given folder corresponding to given device.
    '''
    logger.info('Attempt to mount %s' % path)
    fs = CouchFSDocument(name, path, uri='http://localhost:5984/%s' % name)
    fs.multithreaded = 0
    logger.info('CouchDB Fuse configured for %s' % path)
    fs.main()
    return fs
