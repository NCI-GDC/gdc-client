from setuptools import setup, find_packages

setup(
    name="gdc_client",
    use_scm_version={
        "local_scheme": "dirty-tag",
    },
    classifiers=[
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
    ],
    python_requires=">=3.5",
    setup_requires=["setuptools_scm<6"],
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
        "importlib_metadata<=2.1.3; python_version<'3.8'",  # 2.1.3 last support for py35
        "termcolor~=1.1.0",
        "requests~=2.22.0",
        "progressbar2~=3.43.1",
        "zipp<2",  # zipp < 2 for python3.5
    ],
)
