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
        'setuptools==19.2'
    ],
    dependency_links=[
        'git+https://github.com/LabAdvComp/parcel.git@aba9e1eef1cdda4e6ce22927593c66971a121878#egg=parcel',
    ],
    scripts=[
        'bin/gdc-client',
    ],
)
