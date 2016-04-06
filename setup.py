from setuptools import setup, find_packages
from gdc_client.version import __version__

setup(
    name="gdc_client",
    version=__version__,
    packages=find_packages(),
    package_data={},
    install_requires=[
        'lxml==3.5.0b1',
        'PyYAML==3.11',
        'setuptools==19.2'
    ],
    scripts=[
        'bin/gdc-client',
    ],
)
