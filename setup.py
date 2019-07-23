# Adapted from
# https://github.com/kennethreitz/setup.py

from setuptools import find_packages, setup

# Package meta-data.
NAME = 'bigrays'
DESCRIPTION = 'bigrays is a framework for writing ETL jobs.'
URL = 'https://github.com/CadentTech/bigrays'
REQUIRES_PYTHON = '>=3.6.0'
VERSION = '1.6.0'

# What packages are required for this module to be executed?
REQUIRED = [
    # package: version
    'pandas>=0.23.0,<0.24.0',
]

# These are packages required for non-essential functionality, e.g. loading
# keras models. These additional features can be installed with pip. Below is
# an example of how to install additional keras and s3 functionality.
# 
#    $ pip install bigrays[sql-server,aws]
#
# For more details see:
# http://peak.telecommunity.com/DevCenter/setuptools#declaring-extras-optional-features-with-their-own-dependencies
# and 
# https://github.com/seatgeek/fuzzywuzzy#installation
#
EXTRAS_REQUIRED = {
    'sql-server': ['pyodbc>=4.0.17,<4.1.0', 'SQLAlchemy>=1.1.14,<1.2.0'],
    'aws': ['boto3>=1.7.35,<1.8.0']
}
EXTRAS_REQUIRED['all'] = [r for reqs in EXTRAS_REQUIRED.values() for r in reqs]

# The rest you shouldn't have to touch too much :)
# ------------------------------------------------

# Where the magic happens:
setup(
    name=NAME,
    version=VERSION,
    python_requires=REQUIRES_PYTHON,
    packages=find_packages(exclude=(
        'tests', 'examples',
    )),
    install_requires=REQUIRED,
    extras_require=EXTRAS_REQUIRED
)
