#import random
#import string


def post(client, data, url):
    response = client.post(url, data=data)
    assert response.status_code == 200, "The return code is {r}".format(r=response.status_code)
    return response.data


def get(client, url):
    response = client.get(url)
    assert response.status_code == 200, "The return code is {r}".format(r=response.status_code)
    return response.data


#def random_string(n):
#    letters = string.ascii_lowercase
#    return ''.join(random.choice(letters) for i in range(n))

