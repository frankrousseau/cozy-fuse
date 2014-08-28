# Path helpers
import os

def normalize_path(path):
    '''
    Remove trailing slash and/or empty path part.
    ex: /home//user/ becomes /home/user
    '''
    parts = path.split('/')
    parts = [part for part in parts if part != '']
    path = '/'.join(parts)
    if len(path) == 0:
        return ''
    else:
        return '/' + path

def join(basepath, filename):
    return normalize_path(os.path.join(basepath, filename))
