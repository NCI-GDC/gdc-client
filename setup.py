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
        'git+ssh://git@github.com/LabAdvComp/parcel.git@2bcf5e3c40fe6b11ad51571149f6a89e38ee8453#egg=parcel',
    ],
    scripts=[
        'bin/gdc-client',
    ],
)
