#!/usr/bin/env python

import sys
from setuptools import setup, find_packages

if sys.version_info > (3, 0):
    raise Exception("Only supports Python 2.x")

setup(
    name="HandlerBase",
    version="1.2.1",
    packages=find_packages(),
    install_requires=[""],
)
