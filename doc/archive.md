The archive is the wrapper for accessing data

    class Archive(object):
        __metaclass__ = abc.ABCMeta
    
        @abc.abstractmethod
        def history(self, items, name, before):
            return
            
An archive could merely be a dump for DataFrames:

    from .abc_archive import Archive
    
    class CsvArchive(Archive):
        def __init__(self):
            self.__data = dict()
    
        def put(self, name, frame):
            self.__data[name] = frame
    
        def history(self, items, name, before):
            return self.__data[name][items].truncate(before=before)
        
However, we also offer a wrapper for a MongoDB:
    
    def _f(frame):
        frame.index = [pd.Timestamp(x) for x in frame.index]
        return frame

    class _ArchiveReader(Archive):
        def __init__(self, db, logger=None):
            self.logger = logger or logging.getLogger(__name__)
            self.logger.info("Archive (read-access) at {0}".format(db))
            self.__db = db

    
        def __repr__(self):
            return "Reader for {0}".format(self.__db)
    
        # bad idea to make history a property as we may have different names, e.g PX_LAST, PX_VOLUME, etc...
        def history(self, items=None, name="PX_LAST", before=pd.Timestamp("2002-01-01")):
            collection = self.__db.assets
    
            if items:
                p = collection.find({"_id": {"$in": items}}, {"_id": 1, name: 1})
                frame = pd.DataFrame({x["_id"]: pd.Series(x[name]) for x in p if name in x.keys()})
                for item in items:
                    assert item in frame.keys(), "For asset {0} we could not find series {1}".format(item, name)
    
            else:
                p = collection.find({}, {"_id": 1, name: 1})
                frame = pd.DataFrame({x["_id"]: pd.Series(x[name]) for x in p if name in x.keys()})
    
            return _f(frame).truncate(before=before)
            
