# -*- coding: utf-8 -*-
import pytest
import sys
import os
import errno

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
    create_folder(db, '/A', 'B')
    create_folder(db, '', 'C')

    def fin():
        dbutils.remove_db(name)

    request.addfinalizer(fin)

def test_get_names(config_db):

    fs = couchmount.CouchFSDocument(TESTDB, local_config.MOUNT_FOLDER,
                         'http://localhost:5984/%s' % TESTDB)
    names = fs._get_names('')
    assert names == ['A', 'C', 'file_test.txt']
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
    assert result == ['.', '..', 'A', 'C', 'file_test.txt']


def test_open(config_db):
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


def test_unlink(config_db):
    fs = couchmount.CouchFSDocument(TESTDB, local_config.MOUNT_FOLDER,
                         'http://localhost:5984/%s' % TESTDB)
    path = '/new_file.txt'
    fs.unlink(path)
    db = dbutils.get_db(TESTDB)
    assert dbutils.get_file(db, path) is None
    assert -errno.ENOENT == fs.open(path, 32769)
    assert -errno.ENOENT == fs.getattr(path)
    assert 'new_file.txt' not in fs._get_names('')
    (file_doC, binary_id, filename) = fs.binary_cache.get_file_metadata(path)
    assert not os.path.exists(filename)


def test_mkdir(config_db):
    fs = couchmount.CouchFSDocument(TESTDB, local_config.MOUNT_FOLDER,
                                    'http://localhost:5984/%s' % TESTDB)
    db = dbutils.get_db(TESTDB)
    path = '/new_dir'
    fs.mkdir(path, '')
    folder = dbutils.get_folder(db, path)
    assert folder["path"] == ''
    assert folder["name"] == 'new_dir'


def test_rmdir(config_db):
    fs = couchmount.CouchFSDocument(TESTDB, local_config.MOUNT_FOLDER,
                                    'http://localhost:5984/%s' % TESTDB)
    db = dbutils.get_db(TESTDB)
    path = '/new_dir'
    fs.rmdir(path)
    assert dbutils.get_folder(db, path) is None
    assert -errno.ENOENT == fs.getattr('/new_dir')
    assert 'new_dir' not in fs._get_names('')


def test_rename(config_db):
    fs = couchmount.CouchFSDocument(TESTDB, local_config.MOUNT_FOLDER,
                                    'http://localhost:5984/%s' % TESTDB)
    db = dbutils.get_db(TESTDB)
    pathfrom = '/file_test.txt'
    pathto = '/A/test_doc.txt'
    fs.rename(pathfrom, pathto)

    assert dbutils.get_file(db, pathfrom) is None
    assert -errno.ENOENT == fs.open(pathfrom, 32769)
    assert -errno.ENOENT == fs.getattr(pathfrom)
    assert 'new_file.txt' not in fs._get_names('')

    (file_doc, binary_id, binary_path) = \
        fs.binary_cache.get_file_metadata(pathto)
    assert file_doc['path'] == '/A'
    assert file_doc['name'] == 'test_doc.txt'
    assert -errno.ENOENT != fs.open(pathto, 32769)
    assert -errno.ENOENT != fs.getattr(pathto)

    pathfrom = '/A'
    pathto = '/C'
    fs.rename(pathfrom, pathto)
    assert dbutils.get_file(db, '/A/test.sh') is None
    assert dbutils.get_folder(db, '/A/B') is None
    assert dbutils.get_file(db, '/C/test.sh') is not None
    assert dbutils.get_folder(db, '/C/B') is not None

