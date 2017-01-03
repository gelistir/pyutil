from .abcArchive import Archive

class CsvArchive(Archive):
    """
    Mainly used for unit-testing. This is a very minimalistic Archive providing the same interface to access historic
    data as its siblings.
    """
    def __init__(self):
        self.data = dict()

    def history(self, assets=None, name="PX_LAST"):
        if assets:
            return self.data[name][assets]
        else:
            return self.data[name]
