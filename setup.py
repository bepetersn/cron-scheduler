#!/usr/bin/env python

"""
Easily manage a crontab file through a pythonic 
interface with the simple CronScheduler class--add, 
remove, synchronize, check for existence, reset. 
The class is aware enough to not touch environment 
variables, is tested, and is configurable.

"""
from distutils.core import setup
import setuptools  # this import is needed so that some options and commands work

setup(
    name='cron-scheduler',
    author='Print With Me',
    version='0.0.2',
    include_package_data=True,
    zip_safe=False,
    description=__doc__,
    py_modules=[
        'cron_scheduler',
    ],
    install_requires=[],
)   
