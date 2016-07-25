from setuptools import setup, find_packages
from gdc_client.version import __version__

setup(
    name="gdc_client",
    version=__version__,
    packages=find_packages(),
    package_data={},
    install_requires=[
        'parcel == d3b619584606ed5d2f127f9ba12df7b764ebe975',
        'lxml==3.5.0b1',
        'PyYAML==3.11',
        'setuptools==19.2',
        'jsonschema==2.5.1',
    ],
    dependency_links=[
        'git+https://github.com/LabAdvComp/parcel.git@d3b619584606ed5d2f127f9ba12df7b764ebe975#egg=parcel',
    ],
    scripts=[
        'bin/gdc-client',
    ],
)
