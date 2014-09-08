# -*- coding: utf-8 -*-
import pytest
import sys
import os
import time
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
    dbutils.create_file(db, testfile)


def create_folder(db, path, name):
    testfolder = {
        '_id': uuid4().hex,
        'docType': 'Folder',
        'class': 'folder',
        'path': path,
        'name': name,
        'creationDate': fusepath.get_current_date(),
    }
    dbutils.create_folder(db, testfolder)


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

    couchmount.unmount(local_config.MOUNT_FOLDER)
    time.sleep(1)
    couchmount.mount(name, local_config.MOUNT_FOLDER)
    time.sleep(2)

    def fin():
        pass
        #couchmount.unmount(local_config.MOUNT_FOLDER)
        #time.sleep(10)
        #dbutils.remove_db(name)
    request.addfinalizer(fin)


def test_list_dir(config_db):
    assert os.listdir(local_config.MOUNT_FOLDER) == ['A', 'file_test.txt']
    path = os.path.join(local_config.MOUNT_FOLDER, 'A')
    assert os.listdir(path) == ['test.sh']


#def test_list_dir_special_chars(config_db):
    #db = dbutils.get_db(TESTDB)
    #name = 'Prévisions'.decode('utf-8')
    #path = '/Prévisions'.decode('utf-8')

    #create_folder(db, u'', name)
    #create_folder(db, path, u'B')
    #path = os.path.join(local_config.MOUNT_FOLDER, name)

    #print path
    #print type(path.encode('utf-8'))
    #assert os.listdir(path.encode('utf-8')) == ['B']


def test_read_file(config_db):
    path = os.path.join(local_config.MOUNT_FOLDER, 'file_test.txt')
    testfile = open(path, 'r')
    assert testfile.read() == 'success_test\n'


def test_delete_file(config_db):
    db = dbutils.get_db(TESTDB)
    create_file(db, '', 'to_delete.txt')

    path = os.path.join(local_config.MOUNT_FOLDER, 'to_delete.txt')
    print os.path.exists(path)
    assert os.path.exists(path)
    os.remove(path)
    assert not os.path.exists(path)


#def test_delete_folder(config_db):
    #db = dbutils.get_db(TESTDB)
    #create_folder(db, '', 'C')

    #path = os.path.join(local_config.MOUNT_FOLDER, 'C')
    #assert os.path.exists(path)
    #os.rmdir(path)
    #assert not os.path.exists(path)


#def test_create_file(config_db):
    #path = os.path.join(local_config.MOUNT_FOLDER, 'file_test_2.txt')
    #testfile = open(path, 'w')
    #testfile.write('write_success')
    #testfile.close()
    #assert os.path.exists(path)
    #testfile = open(path, 'r')
    #assert testfile.read() == 'write_success'


#def test_modify_file(config_db):
    #path = os.path.join(local_config.MOUNT_FOLDER, 'file_test_2.txt')
    #testfile = open(path, 'w')
    #testfile.write('write_modification_success')
    #testfile.close()
    #assert os.path.exists(path)
    #testfile = open(path, 'r')
    #assert testfile.read() == 'write_modification_success'


#def test_rename_file(config_db):
    #db = dbutils.get_db(TESTDB)
    #create_folder(db, '/A', 'D')
    #create_folder(db, '', 'C')

    #pathfrom = os.path.join(local_config.MOUNT_FOLDER, 'A', 'test.sh')
    #pathto = os.path.join(local_config.MOUNT_FOLDER, 'C', 'test.sh')
    #os.rename(pathfrom, pathto)

    #path = os.path.join(local_config.MOUNT_FOLDER, 'C', 'test.sh')
    #assert os.path.exists(path)


#def test_rename_folder(config_db):
    #db = dbutils.get_db(TESTDB)

    #pathfrom = os.path.join(local_config.MOUNT_FOLDER, 'A', 'D')
    #pathto = os.path.join(local_config.MOUNT_FOLDER, 'C', 'D')
    #os.rename(pathfrom, pathto)

    #path = os.path.join(local_config.MOUNT_FOLDER, 'C')
    #assert os.path.exists(path)
