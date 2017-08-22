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
        'jsonschema==2.5.1',
        'pyOpenSSL==17.1.0',
        'ndg-httpsclient==0.4.2',
        'pyasn1==0.2.3',
    ],
    dependency_links=[
        'git+https://github.com/LabAdvComp/parcel.git@50d6124a3e3fcd2a234b3373831075390b886a15#egg=parcel',
    ],
    scripts=[
        'bin/gdc-client',
    ],
)
