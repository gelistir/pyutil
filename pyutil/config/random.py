import random
import string


def random_string(n=5):
    return ''.join(random.choice(string.ascii_lowercase) for i in range(n))
