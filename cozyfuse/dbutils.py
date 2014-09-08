import json
import string
import random
import requests
import logging
import datetime

import local_config
import cache
import fusepath


from couchdb import Server, http
from couchdb.http import PreconditionFailed, ResourceConflict

logger = logging.getLogger(__name__)
local_config.configure_logger(logger)

file_cache = cache.Cache()
folder_cache = cache.Cache()
name_cache = cache.Cache()


ATTR_VALIDITY_PERIOD = datetime.timedelta(seconds=10)


def create_db(name):
    '''
    Create a new name for given name.
    '''
    server = Server('http://localhost:5984/')
    try:
        db = server.create(name)
        logger.info('[DB] Database %s created' % name)
    except PreconditionFailed:
        db = server[name]
        logger.info('[DB] Database %s already exists.' % name)

    return db


def get_db(name, credentials=True):
    '''
    Get or create given name from/in CouchDB.
    '''
    try:
        server = Server('http://localhost:5984/')
        if credentials:
            server.resource.credentials = \
                local_config.get_db_credentials(name)
        return server[name]
    except Exception:
        logging.exception('[DB] Cannot connect to the database')

        return None


def get_db_and_server(database):
    '''
    Get or create given database from/in CouchDB.
    '''
    try:
        server = Server('http://localhost:5984/')
        server.resource.credentials = local_config.get_db_credentials(database)
        db = server[database]
        return (db, server)
    except Exception:
        logging.exception('[DB] Cannot connect to the database %s' % database)
        return (None, None)


def init_db(database):
    '''
    Create all required views to make Cozy FUSE working properly.
    '''
    create_db(database)
    init_database_views(database)
    password = get_random_key()
    create_db_user(database, database, password)
    logger.info('[DB] Local database %s initialized' % database)
    return (database, password)


def remove_db(database):
    '''
    Destroy given database.
    '''
    server = Server('http://localhost:5984/')
    try:
        server.delete(database)
    except http.ResourceNotFound:
        logger.info('[DB] Local database %s already removed' % database)
    logger.info('[DB] Local database %s removed' % database)


def get_device(name):
    '''
    Get device corresponding to given name. Device is returned as a dict.
    '''
    try:
        device = list(get_db(name).view("device/all", key=name))[0].value
    except IndexError:
        device = None
    return device


def get_folders(db):
    '''
    Return all folders as a ViewResult list.
    '''
    return db.view("folder/all")


def get_files(db):
    '''
    Return all files as a ViewResult list.
    '''
    return db.view("file/all")


def create_folder(db, folder):
    '''
    Create a new folder and store it in the folder cache (path is the key).
    '''
    folderid = db.create(folder)
    folder = db[folderid]

    dirname, filename = (folder["path"], folder["name"])
    folder_cache.add(fusepath.join(dirname, filename), folder)
    return folder


def get_folder(db, path):
    '''
    Return folder of which path is equal to path. Try to get it from cache
    first.
    '''
    path = fusepath.normalize_path(path)
    try:
        folder = folder_cache.get(path)
        if folder is None:
            folder = list(db.view("folder/byFullPath", key=path))[0].value
            folder_cache.add(path, folder)
    except IndexError:
        folder = None
    return folder


def update_folder(db, folder):
    '''
    Update given folder data. Retrieve last folder revision before doing it to
    avoid conflicts.
    Update folder cache too.
    '''
    current_folder = db[folder["_id"]]
    folder["_rev"] = current_folder["_rev"]
    db.save(folder)

    dirname, filename = (folder["path"], folder["name"])
    newpath = fusepath.join(dirname, filename)

    dirname, filename = (current_folder["path"], current_folder["name"])
    oldpath = fusepath.join(dirname, filename)

    folder_cache.remove(oldpath)
    folder_cache.add(newpath, folder)


def delete_folder(db, folder):
    '''
    Delete given folder and remove it from cache.
    '''
    db.delete(db[folder["_id"]])

    dirname, filename = (folder["path"], folder["name"])
    folder_cache.remove(fusepath.join(dirname, filename))


def create_file(db, file_doc):
    '''
    Create given file and add it to the file cache (key is the file path).
    '''
    fileid = db.create(file_doc)
    file_doc = db[fileid]

    dirname, filename = (fusepath.normalize_path(file_doc["path"]), file_doc["name"])
    file_cache.add(fusepath.join(dirname, filename), file_doc)


def get_file(db, path):
    '''
    Get file located at given path on the Couch FS. Add it to the cache.
    '''
    path = fusepath.normalize_path(path)

    try:
        file_doc = file_cache.get(path)
        if file_doc is None:
            file_doc = list(db.view("file/byFullPath", key=path))[0].value
            file_cache.add(path, file_doc)
    except IndexError:
        file_doc = None
    return file_doc


def update_file(db, file_doc):
    '''
    Ensure file is latest revision then save it to database and update file
    cache accordingly.
    '''
    current_file_doc = db[file_doc["_id"]]
    file_doc["_rev"] = current_file_doc["_rev"]
    db.save(file_doc)

    dirname, filename = (file_doc["path"], file_doc["name"])
    newpath = fusepath.join(dirname, filename)

    dirname, filename = (current_file_doc["path"], current_file_doc["name"])
    oldpath = fusepath.join(dirname, filename)

    file_cache.remove(oldpath)
    file_cache.add(newpath, file_doc)


def delete_file(db, file_doc):
    '''
    Remove given file document from database and from file cache.
    '''
    db.delete(db[file_doc["_id"]])

    dirname, filename = file_doc["path"], file_doc["name"]
    file_cache.remove(fusepath.join(dirname, filename))


def get_random_key():
    '''
    Generate a random key of 20 chars. The first character is not a number
    because CouchDB does not link string that starts with a digit.
    '''
    chars = string.ascii_lowercase + string.digits
    random_val = ''.join(random.choice(chars) for x in range(19))
    return random.choice(string.ascii_lowercase) + random_val


def create_db_user(database, login, password, protocol="http"):
    '''
    Create a user for given *database*. User credentials are *login* and
    *password*.
    '''
    headers = {'content-type': 'application/json'}
    data = {
        "_id": "org.couchdb.user:%s" % login,
        "name": login,
        "type": "user",
        "roles": [],
        "password": password
    }
    requests.post('%s://localhost:5984/_users' % (protocol),
                  data=json.dumps(data),
                  headers=headers,
                  verify=False)

    headers = {'content-type': 'application/json'}
    data = {
        "admins": {
            "names": [login],
            "roles": []
        },
        "members": {
            "names": [login],
            "roles": []
        },
    }
    requests.put('%s://localhost:5984/%s/_security' % (protocol, database),
                 data=json.dumps(data),
                 headers=headers,
                 verify=False)
    logger.info('[DB] Db user created')


def remove_db_user(database):
    '''
    Delete user created for this database.
    '''
    response = requests.get(
        'http://localhost:5984/_users/org.couchdb.user:%s' % database)
    rev = response.json().get("_rev", "")

    response = requests.delete(
        'http://localhost:5984/_users/org.couchdb.user:%s?rev=%s' %
        (database, rev)
    )
    logger.info('[DB] Db user %s deleted' % database)


def init_database_view(docType, db):
    '''
    Add view in database for given docType.
    '''
    db["_design/%s" % docType.lower()] = {
        "views": {
            "all": {
                "map": """function (doc) {
                              if (doc.docType === \"%s\") {
                                  emit(doc._id, doc)
                              }
                           }""" % docType
            },
            "byFolder": {
                "map": """function (doc) {
                              if (doc.docType === \"%s\") {
                                  emit(doc.path, doc)
                              }
                          }""" % docType
            },
            "byFullPath": {
                "map": """function (doc) {
                  if (doc.docType === \"%s\") {
                      emit(doc.path + '/' + doc.name, doc);
                    }
                  }""" % docType
            }
        },
        "filters": {
            "all": """function (doc, req) {
                          return doc.docType === \"%s\"
                      }""" % docType
        }
    }


def init_database_views(database):
    '''
    Initialize database:
        * Create database
        * Initialize folder, file, binary and device views
    '''
    db = get_db(database, credentials=False)

    try:
        init_database_view('Folder', db)
        logger.info('[DB] Folder design document created')
    except ResourceConflict:
        logger.warn('[DB] Folder design document already exists')

    try:
        init_database_view('File', db)
        logger.info('[DB] File design document created')
    except ResourceConflict:
        logger.warn('[DB] File design document already exists')

    try:
        db["_design/device"] = {
            "views": {
                "all": {
                    "map": """function (doc) {
                                  if (doc.docType === \"Device\") {
                                      emit(doc.login, doc)
                                  }
                              }"""
                },
                "byUrl": {
                    "map": """function (doc) {
                                  if (doc.docType === \"Device\") {
                                      emit(doc.url, doc)
                                  }
                              }"""
                }
            }
        }
        logger.info('[DB] Device design document created')
    except ResourceConflict:
        logger.warn('[DB] Device design document already exists')

    try:
        db["_design/binary"] = {
            "views": {
                "all": {
                    "map": """function (doc) {
                                  if (doc.docType === \"Binary\") {
                                      emit(doc._id, doc)
                                  }
                               }"""
                }
            }
        }
        logger.info('[DB] Binary design document created')
    except ResourceConflict:
        logger.warn('[DB] Binary design document already exists')


def init_device(database, url, path, device_pwd, device_id):
    '''
    Create device objects wiht filter to apply to synchronize them.
    '''
    db = get_db(database)
    device = get_device(database)

    # Update device
    device['password'] = device_pwd
    device['change'] = 0
    device['url'] = url
    device['folder'] = path
    device['configuration'] = ["File", "Folder", "Binary"]
    db.save(device)

    # Generate filter
    conditions = "(doc.docType && ("
    for docType in device["configuration"]:
        conditions += 'doc.docType === "%s" || ' % docType
    conditions = conditions[0:-3] + '))'

    first_filter = """function(doc, req) {
        if(doc._deleted || %s) {
            return true;
        } else {
            return false;
        }
    }""" % conditions

    doctype_filter = """function(doc, req) {
        if (%s) {
            return true;
        } else {
            return false;
        }
    }""" % conditions

    doc = {
        "_id": "_design/%s" % device_id,
        "views": {},
        "filters": {
            "filter": first_filter,
            "filterDocType": doctype_filter
        }
    }

    try:
        db.save(doc)
        logger.info('[DB] Device filter created for device %s' % database)
    except ResourceConflict:
        logger.warn('[DB] Device filter document already exists')

    return False


def get_disk_space(database, url, device, device_password):
    # Recover disk space
    db = get_db(database)
    url = url.split('/')
    try:
        remote = "https://%s:%s@%s" % (device, device_password, url[2])
        response = requests.get('%s/disk-space'%remote)
        disk_space = json.loads(response.content)
        # Store disk space
        res = db.view('device/all')
        for device in res:
            device = device.value
            device['diskSpace'] = disk_space['diskSpace']
            db.save(device)
            # Return disk space
            return disk_space['diskSpace']
    except:
        # Recover information in database
        res = db.view('device/all')
        for device in res:
            device = device.value
            if 'diskSpace' in device:
                return device['diskSpace']
            else:
                # Return arbitrary information
                disk_space = {
                    "freeDiskSpace": 1,
                    "usedDiskSpace": 0,
                    "totalDiskSpace": 1
                }
                return disk_space
