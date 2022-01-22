#!/usr/bin/env python
from setuptools import setup

with open("README.md", "r", encoding="utf-8") as f:
    readme = f.read()

setup(
    name='target-miso',
    version='0.1.0',
    description='Singer.io target for writing data to miso api',
    long_description=readme,
    long_description_content_type='text/markdown',
    author='Hash Lin',
    author_email="hashlin@askmiso.com",
    url="https://gitlab.com/askmiso/target-miso",
    keywords=["singer", "singer.io", "target", "etl"],
    classifiers=['Programming Language :: Python :: 3 :: Only'],
    py_modules=['target_miso'],
    install_requires=['urllib3==1.26.7', 'singer-python==5.12.2', 'requests==2.26.0',
                      'jinja2==3.0.3', 'sentry-sdk==1.5.0',
                      'toolz==0.11.2'],
    entry_points='''
          [console_scripts]
          target-miso=target_miso:main
      ''',
)
