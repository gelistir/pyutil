from .abcArchive import Archive

class CsvArchive(Archive):
    """
    Mainly used for unit-testing. This is a very minimalistic Archive providing
    the same interface to access historic data as its siblings. Note that after initialization the underlying
    data remains immutable. You can only access copies of the original data. You may pay some performance penalty
    for it but we sacrifice some performance for safety here...
    """
    def __init__(self, **kwargs):
        self.__data = {key: item for key, item in kwargs.items()}


    def history(self, assets=None, name="PX_LAST"):
        if assets:
            return self.__data[name][assets].copy()
        else:
            return self.__data[name].copy()
