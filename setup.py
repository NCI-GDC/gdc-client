from setuptools import setup, find_packages
from gdc_client.version import __version__

setup(
    name="gdc_client",
    version=__version__,
    packages=find_packages(),
    package_data={},
    scripts=["bin/gdc-client"],
    install_requires=[
        "cryptography~=2.8",
        "jsonschema~=2.6.0",
        "lxml~=4.4.2",
        "ndg-httpsclient~=0.5.0",
        "pyasn1~=0.4.3",
        "pyOpenSSL~=18.0.0",
        "PyYAML>=5.1",
        "intervaltree~=3.0.2",
        "termcolor~=1.1.0",
        "requests~=2.22.0",
        "progressbar2~=3.43.1",
    ],
)
