import random
import string


def random_string(n=5):
    # create a random string of length n
    return ''.join(random.choice(string.ascii_lowercase) for i in range(n))
