from setuptools import setup, find_packages
from gdc_client.version import __version__

setup(
    name="gdc_client",
    version=__version__,
    packages=find_packages(),
    package_data={},
    install_requires=[
        'parcel',
    ],
    dependency_links=[
        'git+ssh://git@github.com/LabAdvComp/parcel.git@9f2e3bd6769366a1bd99d37fb0971d83e333553e#egg=parcel',
    ],
    scripts=[
        'bin/gdc-client',
    ],
)
