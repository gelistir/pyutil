from pyutil.web.aux import rest2data, reset_index, double2percent
from pyutil.web.post import post_month, post_perf


def month(request):
    # extract data of incoming request
    data = rest2data(request)
    # construct the frame, return the "data, columns" dictionary...
    return reset_index(frame=post_month(data).applymap(double2percent), index="Year")

# very hard to test outside
def performance(request):
    data = rest2data(request)
    x = post_perf(data)
    return {"data": [{"name": name, "value": value} for name, value in x.items()]}