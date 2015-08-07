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
        'git+ssh://git@github.com/LabAdvComp/parcel.git@9640a606f1000044949a3fb7a47bb516926334c9#egg=parcel',
    ],
    scripts=[
        'bin/gdc-client',
    ],
)
