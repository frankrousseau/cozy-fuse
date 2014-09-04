# Path helpers
import os
import datetime
import calendar
import ntpath

def normalize_path(path):
    '''
    Remove trailing slash and/or empty path part.
    ex: /home//user/ becomes /home/user
    '''
    parts = path.split('/')
    parts = [part for part in parts if part != '']
    path = '/'.join(parts)
    if type(path) is str:
        path = path.decode('utf-8')

    if len(path) == 0:
        return u''
    else:
        return u'/' + path

def join(basepath, filename):
    return normalize_path(os.path.join(basepath, filename))


def split(path):
    folder_path, name = ntpath.split(path)
    return normalize_path(folder_path), name

def get_current_date():
    """
    Get current date : Return current date with format 'Y-m-d T H:M:S'
        Exemple : 2014-05-07T09:17:48
    """
    return datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')


def get_date(ctime):
    ctime = ctime[0:24]
    try:
        date = datetime.datetime.strptime(ctime, "%Y-%m-%dT%H:%M:%S")
    except ValueError:
        try:
            date = datetime.datetime.strptime(ctime, "%Y-%m-%dT%H:%M:%S.%fZ")
        except ValueError:
            try:
                date = datetime.datetime.strptime(
                    ctime,
                    "%a %b %d %Y %H:%M:%S")
            except ValueError:
                date = datetime.datetime.strptime(
                    ctime,
                    "%a %b %d %H:%M:%S %Y")
    return calendar.timegm(date.utctimetuple())
