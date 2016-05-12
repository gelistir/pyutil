from pyutil.mongo.archive import reader

if __name__ == '__main__':
    r = reader(name="production")
    n = r.portfolios.nav["LOBNEK CONSERVATIVE"].dropna()
    print(n)

    n2 = r.read_nav("LOBNEK CONSERVATIVE").series
    print(n.head(10))
    print(n2.head(10))
    print((n2/n).dropna())