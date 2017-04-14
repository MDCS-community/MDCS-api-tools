from setuptools import setup, find_packages

with open('README.md') as f:
    readme = f.read()

with open('LICENSE') as f:
    license = f.read()

setup(
    name='mdcs',
    version='0.0.2',
    description='Library for simplifying calls to MDCS REST API',
    long_description=readme,
    url='https://github.com/MDCS-community/MDCS-api-tools',
    license=license,
    install_requires=[
        'requests',
        'pandas',
        'xmltodict',
        'lxml',
    ],
    packages=find_packages(exclude=('tests', 'docs'))
)
