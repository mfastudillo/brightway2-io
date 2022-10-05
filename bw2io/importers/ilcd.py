from .base_lci import LCIImporter
from ..extractors.ilcd import ILCDExtractor

class ILCDImporter(LCIImporter):

    def __init__(self,dirpath,dbname):

        self.dbname = dbname
        self.data = ILCDExtractor._extract(dirpath)