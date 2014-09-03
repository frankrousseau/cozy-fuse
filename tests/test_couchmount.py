# -*- coding: utf-8 -*-
import pytest
import sys
import os
from uuid import uuid4

sys.path.append('..')

import cozyfuse.local_config as local_config
local_config.CONFIG_FOLDER = \
    os.path.join(os.path.expanduser('~'), '.cozyfuse-test')

local_config.CONFIG_PATH = \
    os.path.join(local_config.CONFIG_FOLDER, 'config.yaml')

local_config.MOUNT_FOLDER = \
    os.path.join(os.path.expanduser('~'), '.cozyfuse-test', 'mounted')
if not os.path.exists(local_config.MOUNT_FOLDER):
    os.makedirs(local_config.MOUNT_FOLDER)

import cozyfuse.dbutils as dbutils
import cozyfuse.couchmount as couchmount
import cozyfuse.fusepath as fusepath

TESTDB = 'cozy-fuse-test'
MOUNT_FOLDER = os.path.join(os.path.expanduser('~'), TESTDB)
DEVICE_CONFIG_PATH = os.path.join(local_config.CONFIG_FOLDER, TESTDB)
CACHE_FOLDER = os.path.join(DEVICE_CONFIG_PATH, 'cache')
COUCH_URL = 'http://login:password@localhost:5984/cozy-fuse-test'
BINARY_ID = uuid4().hex
FILE_ID = uuid4().hex


def create_file(db, path, name):
    testfile = {
        '_id': uuid4().hex,
        'docType': 'File',
        'class': 'file',
        'path': path,
        'name': name,
        'size': 10,
        'mime': 'text/plain',
        'creationDate': fusepath.get_current_date(),
        'binary': { 'file': { 'id': BINARY_ID } }
    }
    db.save(testfile)


def create_folder(db, path, name):
    testfolder = {
        '_id': uuid4().hex,
        'docType': 'Folder',
        'class': 'folder',
        'path': path,
        'name': name,
        'creationDate': fusepath.get_current_date(),
    }
    db.save(testfolder)


@pytest.fixture(scope="module")
def config_db(request):
    filename = local_config.CONFIG_PATH
    with file(filename, 'a'):
        os.utime(filename, None)

    name = TESTDB
    url = 'https://localhost:2223'
    path = '/home/myself/cozyfiles'
    db_login = 'login'
    db_password = 'password'
    dbutils.remove_db(name)
    dbutils.create_db(name)
    dbutils.create_db_user(name, db_login, db_password)
    local_config.add_config(name, url, path, db_login, db_password)
    db = dbutils.get_db(name)
    dbutils.init_database_views(name)

    device = {
        '_id': uuid4().hex,
        'docType': 'Device',
        'login': TESTDB,
        'url': 'http://',
        'password': TESTDB,
    }
    db.save(device)

    binary = {
       '_id': BINARY_ID,
        'docType': 'Binary',
    }
    db.save(binary)
    db.put_attachment(binary, open('./file_test.txt'), 'file')

    create_file(db, '', 'file_test.txt')
    create_file(db, '/A', 'test.sh')
    create_folder(db, '', 'A')

    def fin():
        dbutils.remove_db(name)

    request.addfinalizer(fin)

def test_get_names(config_db):

    fs = couchmount.CouchFSDocument(TESTDB, local_config.MOUNT_FOLDER,
                         'http://localhost:5984/%s' % TESTDB)
    names = fs._get_names('')
    assert names == ['file_test.txt', 'A']
    assert fs._is_in_list_cache('/A')
    assert not fs._is_in_list_cache('/B')


def test_getattr(config_db):
    fs = couchmount.CouchFSDocument(TESTDB, local_config.MOUNT_FOLDER,
                         'http://localhost:5984/%s' % TESTDB)
    attr = fs.getattr('/file_test.txt')
    assert attr.st_size == 10
    assert attr.st_nlink == 1
    attr = fs.getattr('/A')
    assert attr.st_nlink == 2


def test_readdir(config_db):
    fs = couchmount.CouchFSDocument(TESTDB, local_config.MOUNT_FOLDER,
                         'http://localhost:5984/%s' % TESTDB)
    result = [name.name for name in fs.readdir('/', 0)]
    assert result == ['.', '..', 'file_test.txt', 'A']


def test_open(config_db):
    import errno
    fs = couchmount.CouchFSDocument(TESTDB, local_config.MOUNT_FOLDER,
                         'http://localhost:5984/%s' % TESTDB)
    assert 0 == fs.open('/file_test.txt', 32769)
    assert -errno.ENOENT == fs.open('/file_testa.txt', 32769)

def test_mknod(config_db):
    fs = couchmount.CouchFSDocument(TESTDB, local_config.MOUNT_FOLDER,
                         'http://localhost:5984/%s' % TESTDB)
    path = '/new_file.txt'
    fs.mknod(path, 'w', '')
    (file_doc, binary_id, binary_path) = \
        fs.binary_cache.get_file_metadata(path)
    assert file_doc['path'] == ''
    assert file_doc['name'] == 'new_file.txt'
    assert os.path.exists(binary_path)

def test_write(config_db):
    fs = couchmount.CouchFSDocument(TESTDB, local_config.MOUNT_FOLDER,
                         'http://localhost:5984/%s' % TESTDB)
    path = '/new_file.txt'
    fs.write(path, 'test_write', 0)
    with fs.binary_cache.get(path) as binary:
        content = binary.read()
        assert 'test_write' == content
    fs.write(path, '_again', len('test_write'))
    with fs.binary_cache.get(path) as binary:
        content = binary.read()
        assert 'test_write_again' == content

def test_release(config_db):
    fs = couchmount.CouchFSDocument(TESTDB, local_config.MOUNT_FOLDER,
                         'http://localhost:5984/%s' % TESTDB)
    path = '/new_file.txt'
    fs.release(path, None)

    db = dbutils.get_db(TESTDB)
    file_doc = dbutils.get_file(db, path)
    assert file_doc['size'] == len('test_write_again')
