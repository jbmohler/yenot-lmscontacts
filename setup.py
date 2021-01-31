#!/usr/bin/env python

from distutils.core import setup

setup(
    name="lmscontacts",
    version="0.1",
    description="Contacts on Yenot",
    author="Joel B. Mohler",
    author_email="joel@kiwistrawberry.us",
    url="https://bitbucket.org/jbmohler/lmscontacts",
    packages=["lcserver"],
    install_requires=["yenot", "cryptography"],
)
