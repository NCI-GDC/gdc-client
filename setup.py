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
        'git+ssh://git@github.com/LabAdvComp/parcel.git@c51523de7088208ac6a559283644035f3ea1ea7b#egg=parcel',
    ],
    scripts=[
        'bin/gdc-client',
    ],
)
