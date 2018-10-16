# -*- coding: utf-8 -*-

from setuptools import setup
from setuptools import find_packages

REQUIRES = [
    'requests==2.18.4',
    'retrying==1.3.3',
]


def read(fname):
    with open(fname) as fp:
        content = fp.read()
    return content


setup(
    name='directaccess',
    version='0.4.1',
    description='Drillinginfo Direct Access API Python Client',
    long_description=read('README.md'),
    author='Cole Howard',
    author_email='wchatx@gmail.com',
    packages=find_packages(exclude=('test*', )),
    package_dir={'directaccess': 'directaccess'},
    install_requires=REQUIRES,
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3.6',
    ],
)
