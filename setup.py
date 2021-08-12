#!/usr/bin/env python
from setuptools import setup

setup(
    name="tap-woocommerce-v3",
    version="0.3.4",
    description="Singer.io tap for extracting data",
    author="Stitch",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_woocommerce"],
    install_requires=[
        'unidecode==1.2.0',
        'attrs==18.1.0',
        'backoff==1.3.2',
        'python-dateutil==2.7.3',
        'requests==2.20.0',
        "pendulum",
        'singer-python==5.0.15',
    ],
    entry_points="""
    [console_scripts]
    tap-woocommerce-v3=tap_woocommerce:main
    """,
    packages=["tap_woocommerce"],
    package_data = {
        "schemas": ["tap_woocommerce/schemas/*.json"]
    },
    include_package_data=True,
)
