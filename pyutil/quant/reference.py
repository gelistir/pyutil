from sqlalchemy.sql.elements import or_

from pyutil.sql.interfaces.ref import Field, FieldType


def __read_reference(reader, tickers, fields):
    for key, row in reader(fields=fields, tickers=tickers).iterrows():
        yield row["ticker"], row["field"], str(row["value"])


def __assets_fields(symbols, fields):
    assets = {symbol.name: symbol for symbol in symbols}
    fields = {field.name: field for field in fields.filter(or_(Field.type == FieldType.dynamic,
                                                                    Field.type == FieldType.static))}

    return assets, fields


def update_reference(symbols, fields, reader):
    assets, fields = __assets_fields(symbols=symbols, fields=fields)

    for ticker, field, value in __read_reference(reader=reader, fields=list(fields.keys()), tickers=list(assets.keys())):
        assets[ticker].reference[fields[field]] = str(value)
