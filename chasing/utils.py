# chasing/utils.py
import hashlib

from django.conf import settings


def h(value):
    """
    Generate a SHA1 hash for the input value.
    Salted with the project's 'SECRET_KEY'.
    """
    encoded_value = (value + settings.SECRET_KEY).encode()
    return hashlib.sha1(encoded_value).hexdigest()

