#!/usr/bin/env python

from setuptools import setup, find_packages

setup(name='tap-recharge',
      version='1.0.1',
      description='Singer.io tap for extracting data from the ReCharge Payments API 2.0',
      author='jeff.huth@bytecode.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_recharge'],
      install_requires=[
          'backoff==1.8.0',
          'requests==2.20.0',
          'singer-python==5.8.0'
      ],
      entry_points='''
          [console_scripts]
          tap-recharge=tap_recharge:main
      ''',
      packages=find_packages(),
      package_data={
          'tap_recharge': [
              'schemas/*.json'
          ]
      })
