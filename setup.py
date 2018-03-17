from setuptools import setup

def readme():
    with open('README.md') as f:
        return f.read();

def license():
    with open('LICENSE') as f:
        return f.read();

setup(
    name='hpc-instagram-geoprocessing',
    version='0.0.1',
    description='COMP90024 2018S1 Assignment 1',
    long_description=readme(),
    url='https://github.com/eyeonechi/hpc-instagram-geoprocessing',
    author='Ivan Ken Weng Chee',
    author_email='ichee@student.unimelb.edu.au',
    license=license(),
    keywords=[
        'COMP90024'
    ],
    scripts=[
        'bin/hpc_instagram_geoprocessing'
    ],
    packages=[],
    zip_safe=False,
    include_package_data=True
)
