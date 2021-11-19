#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import find_packages, setup

import abstractqueue


setup(
    name="abstractqueue",
    version=abstractqueue.__version__,
    packages=find_packages(),
    author="Laurent Evrard",
    description="Queuing tools for python",
    long_description=open("README.md").read(),
    install_requires=["redis", "pika"],
    url="https://github.com/owlint/abstractqueue",
    classifiers=[
        "Programming Language :: Python",
        "Development Status :: 1 - Planning",
        "License :: OSI Approved",
        "Natural Language :: French",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3.10",
        "Topic :: Communications",
    ],
    license="Apache License 2.0",
)
