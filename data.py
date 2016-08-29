from pyutil.mongo.archive import reader

if __name__ == '__main__':
    r = reader(name="production", host="quantsrv")
    for key in r.portfolios.keys():
        try:
            r.portfolios[key]
        except:
            print(key)
