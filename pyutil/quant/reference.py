from pyutil.sql.interfaces.symbols.frames import Frame
from pyutil.sql.interfaces.symbols.symbol import Symbol
from pyutil.sql.model.ref import Field, FieldType
from pyutil.sql.session import get_one_or_create
from sqlalchemy.sql.elements import or_


def __read_reference(reader, tickers, fields):
    for key, row in reader(fields=fields, tickers=tickers).iterrows():
        yield row["ticker"], row["field"], str(row["value"])


def frame(session, name="Reference"):
    f, exists = get_one_or_create(session=session, model=Frame, name=name)
    x = Symbol.reference_frame(session.query(Symbol))

    # change the index
    x.index = [a.name for a in x.index]

    #assert isinstance(f, Frame)

    f.frame = x

    return f.frame


def __assets_fields(symbols, fields):
    assets = {symbol.name: symbol for symbol in symbols}
    fields = {field.name: field for field in fields.filter(or_(Field.type == FieldType.dynamic,
                                                                    Field.type == FieldType.static))}

    return assets, fields


def update_reference(symbols, fields, reader):
    assets, fields = __assets_fields(symbols=symbols, fields=fields)

    for ticker, field, value in __read_reference(reader=reader, fields=list(fields.keys()), tickers=list(assets.keys())):
        assets[ticker].reference[fields[field]] = str(value)
