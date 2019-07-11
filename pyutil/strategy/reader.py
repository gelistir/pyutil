from pyutil.sql.interfaces.symbols.symbol import Symbol


def assets(session, names=None):
    if names is None:
        return session.query(Symbol).all()
    else:
        return session.query(Symbol).filter(Symbol.name.in_(names)).all()


def symbolmap(session, names=None):
    return {asset.name: asset.group.name for asset in assets(session, names)}

