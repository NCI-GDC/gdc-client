from setuptools import setup, find_packages
from gdc_client.version import __version__

setup(
    name="gdc_client",
    version=__version__,
    packages=find_packages(),
    package_data={},
    install_requires=[
        'parcel',
        'lxml==3.5.0b1',
        'PyYAML==3.11',
        'setuptools==19.2',
        'jsonschema==2.5.1',
    ],
    dependency_links=[
        'git+https://github.com/LabAdvComp/parcel.git@8848d51da7fae71b3112ead49e20a90700aa6441#egg=parcel',
    ],
    scripts=[
        'bin/gdc-client',
    ],
)
