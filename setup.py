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
        'git+https://github.com/LabAdvComp/parcel.git@596d779c814198a9e72a8b7a28dc0511df814467#egg=parcel',
    ],
    scripts=[
        'bin/gdc-client',
    ],
)
