def read_prices(collection, kind="PX_LAST"):
    return collection.frame(key="name", kind=kind)


def read_price(collection, name, parse=True, kind="PX_LAST"):
    return collection.find_one(name=name, parse=parse, kind=kind)


def write_price(collection, data, name, kind="PX_LAST"):
    collection.insert(p_obj=data, name=name, kind=kind)
