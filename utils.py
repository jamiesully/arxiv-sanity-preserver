from contextlib import contextmanager

import os
import re
import pickle
import tempfile

# global settings
# -----------------------------------------------------------------------------
class Config(object):
    # main paper information repo file
    db_path = 'db.p'
    
    # intermediate processing folders
    pdf_dir = os.path.join('data', 'pdf')
    txt_dir = os.path.join('data', 'txt')
    thumbs_dir_local = os.path.join('static', 'thumbs')
    try:
        cloudinary_path = os.environ["CLOUDINARY_URL"]
    except:
        cloudinary_path = os.popen("heroku config:get CLOUDINARY_URL -a hepthio").read()[:-1]
    cloudinary_cloud_name = cloudinary_path.split('@')[1]
    cloudinary_api_key = cloudinary_path.split(':')[1][2:]
    cloudinary_api_secret = cloudinary_path.split(':')[2].split('@')[0]
    
    thumbs_dir = 'http://res.cloudinary.com/'+cloudinary_cloud_name+'/raw/upload/thumbs/'
    
    # intermediate pickles
    tfidf_path = 'tfidf.p'
    meta_path = 'tfidf_meta.p'
    sim_path = 'sim_dict.p'
    user_sim_path = 'user_sim.p'
    search_dict_path = 'search_dict.p'
    serve_cache_path = 'serve_cache.p'
    
    # sql database file
    db_serve_path = 'db2.p' # an enriched db.p with various preprocessing info
    database_path = 'as.db'
    try:
        heroku_database_path = os.environ["DATABASE_URL"]
    except:
        heroku_database_path = DATABASE_URL=os.popen("heroku config:get DATABASE_URL -a hepthio").read()[:-1]
    
    # Heroku mongo paths and variables
    try:
        heroku_mongo_path = os.environ["MONGODB_URI"]
    except:
        heroku_mongo_path = DATABASE_URL=os.popen("heroku config:get MONGODB_URI -a hepthio").read()[:-1]
    mongo_db_name= heroku_mongo_path.split('/')[-1]

    banned_path = 'banned.txt' # for twitter users who are banned
    tmp_dir = 'tmp'

# Context managers for atomic writes courtesy of
# http://stackoverflow.com/questions/2333872/atomic-writing-to-file-with-python
@contextmanager
def _tempfile(*args, **kws):
    """ Context for temporary file.

    Will find a free temporary filename upon entering
    and will try to delete the file on leaving

    Parameters
    ----------
    suffix : string
        optional file suffix
    """

    fd, name = tempfile.mkstemp(*args, **kws)
    os.close(fd)
    try:
        yield name
    finally:
        try:
            os.remove(name)
        except OSError as e:
            if e.errno == 2:
                pass
            else:
                raise e


@contextmanager
def open_atomic(filepath, *args, **kwargs):
    """ Open temporary file object that atomically moves to destination upon
    exiting.

    Allows reading and writing to and from the same filename.

    Parameters
    ----------
    filepath : string
        the file path to be opened
    fsync : bool
        whether to force write the file to disk
    kwargs : mixed
        Any valid keyword arguments for :code:`open`
    """
    fsync = kwargs.pop('fsync', False)

    with _tempfile(dir=os.path.dirname(filepath)) as tmppath:
        with open(tmppath, *args, **kwargs) as f:
            yield f
            if fsync:
                f.flush()
                os.fsync(file.fileno())
        os.replace(tmppath, filepath)

def safe_pickle_dump(obj, fname):
    with open_atomic(fname, 'wb') as f:
        pickle.dump(obj, f, -1)


# arxiv utils
# -----------------------------------------------------------------------------

def strip_version(idstr):
    """ identity function if arxiv id has no version, otherwise strips it. """
    parts = idstr.split('v')
    return parts[0]

# "1511.08198v1" is an example of a valid arxiv id that we accept
def isvalidid(pid):
  return re.match('^([a-z]+(-[a-z]+)?/)?\d+(\.\d+)?(v\d+)?$', pid)
