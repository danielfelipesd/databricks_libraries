from gettext import install
from setuptools import setup, find_packages

classifiers = [
    'Development status :: 5 Production/Stable'
    , 'Intended Audience :: '
    , 'Operating System :: Microsoft :: Windows :: Windows 10'
    , 'License :: OSI Approved :: MIT License'
    , 'Programming Language :: Python :: 3'
]

setup(
    name ="tada_utils"
    , version = "0.0.1"
    , description= ""
    , long_description = open("README.txt").read() + '\n\n' + open("CHANGELOG.txt").read()
    , url = ""
    , author = "Daniel"
    , author_email = "Daniel.Oliveira-ext2@ab-Inbev.com"
    , license = "MIT"
    , classifiers = classifiers
    , keywords = ""
    , packages = find_packages()
    , install_requeriment = [\
        "azure-storage-file-datalake"\
        , "azure-identity"\
        , "azure-data-tables"\
        , "snowflake-connector-python"\
        , "re"\
        , "requests"\
        , "io"\
        , "logging"\
        , "datetime"\
        , "logging_json"\
        , "json"\
        , "slack-sdk"\
    ]
)