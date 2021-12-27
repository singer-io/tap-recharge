#!/usr/bin/env python

from setuptools import setup, find_packages

setup(name='tap-recharge',
      version='1.1.2',
      description='Singer.io tap for extracting data from the ReCharge Payments API 2.0',
      author='jeff.huth@bytecode.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_recharge'],
      install_requires=[
          'backoff==1.8.0',
          'requests==2.25.1',
          'singer-python==5.12.2'
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
      },
      extras_require={
          'dev': [
              'pylint'
          ]
      })
