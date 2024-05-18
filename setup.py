from setuptools import setup

setup(
    name='kubegrapher',
    version='0.1',
    description='an ETL pipeline that pulls data from kubernetes, transforms it, and stores it in a graph database',
    author='Tarek Zaarour',
    author_email='tarek.zaarour@dell.com',
    packages=['kubegrapher'],
    zip_safe=False
)