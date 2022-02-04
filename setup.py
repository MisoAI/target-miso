#!/usr/bin/env python
from setuptools import setup

with open("README.md", "r", encoding="utf-8") as f:
    readme = f.read()

setup(
    name='target-miso',
    version='0.1.1',
    description='Singer.io target for writing data to miso api',
    long_description=readme,
    long_description_content_type='text/markdown',
    author='Hash Lin',
    author_email="hashlin@askmiso.com",
    url="https://gitlab.com/askmiso/target-miso",
    keywords=["singer", "singer.io", "target", "etl"],
    classifiers=['Programming Language :: Python :: 3 :: Only'],
    packages=['target_miso'],
    install_requires=['singer-python', 'requests',
                      'jsonnet', 'sentry-sdk', 'simplejson'],
    entry_points='''
          [console_scripts]
          target-miso=target_miso:main
      ''',
)
