In this brief note I would like to discuss an efficient interface between Pandas and sqlAlchemy.
I assume you are familiar with basic sql concepts. Often when you store time series data you end up with storing
rows of triples. Each row will contain a date, a value and the id of the time series, e.g:

    class _TimeseriesData(_Base):
        __tablename__ = 'ts_data'
        date = sq.Column(sq.Date, primary_key=True)
        value = sq.Column(sq.Float)
        _ts_id = sq.Column("ts_id", sq.Integer, sq.ForeignKey(_Timeseries._id), primary_key=True)
    
        def __init__(self, date=None, value=None):
            self.date = date
            self.value = value
            
The actual Timeseries is defined as

    class _Timeseries(_Base):
        __tablename__ = 'ts_name'
        _id = sq.Column("id", sq.Integer, primary_key=True, autoincrement=True)
    
        name = sq.Column(sq.String(50), unique=True)
    
        def __init__(self, name=None, data=None):
            self.name = name
            if data is not None:
                self.upsert(data)
    
        @property
        def series(self):
            return _pd.Series({date: x.value for date, x in self._data.items()})
    
        def upsert(self, ts):
            for date, value in ts.items():
                self.data[date] = value
    
            return self