from setuptools import setup, find_packages

# read the contents of your README file
from pathlib import Path

this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text()

setup(
    name="gdc_client",
    long_description=long_description,
    long_description_content_type="text/markdown",
    use_scm_version={
        "local_scheme": "dirty-tag",
    },
    setup_requires=["setuptools_scm<8"],
    packages=find_packages(),
    package_data={},
    scripts=["bin/gdc-client"],
    install_requires=[
        "cryptography",
        "jsonschema~=2.6.0",
        "lxml~=4.4.2",
        "ndg-httpsclient~=0.5.0",
        "pyasn1",
        "pyOpenSSL",
        "PyYAML>=5.1",
        "intervaltree~=3.0.2",
        "importlib_metadata",
        "termcolor~=1.1.0",
        "requests~=2.22.0",
        "progressbar2~=3.43.1",
    ],
)
