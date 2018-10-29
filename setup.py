#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import sys
from setuptools import setup
from setuptools import find_packages
from setuptools.command.install import install

VERSION = '0.4.6'

REQUIRES = [
    'requests',
    'retrying==1.3.3',
]


class VerifyVersionCommand(install):
    description = 'verify that version matches the tag prior to circleci pushing to pypi'

    def run(self):
        tag = os.getenv('CIRCLE_TAG')

        if tag != VERSION:
            info = 'Git tag: {0} does not match the version of this app: {1}'.format(
                tag, VERSION
            )
            sys.exit(info)


def read(fname):
    with open(fname) as fp:
        content = fp.read()
    return content


setup(
    name='directaccess',
    version=VERSION,
    description='Drillinginfo Direct Access API Python Client',
    long_description=read('README.md'),
    long_description_content_type='text/markdown',
    author='Cole Howard',
    author_email='wchatx@gmail.com',
    url='https://github.com/wchatx/direct-access-py',
    license='MIT',
    keywords=['drillinginfo', 'oil', 'gas'],
    packages=find_packages(exclude=('test*', )),
    package_dir={'directaccess': 'directaccess'},
    install_requires=REQUIRES,
    cmdclass={
        'verify': VerifyVersionCommand,
    },
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Intended Audience :: End Users/Desktop',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5'
        'Programming Language :: Python :: 3.6',
    ]
)
