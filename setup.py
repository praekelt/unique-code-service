from setuptools import setup

setup(
    name="unique-code-service",
    version="0.1.0",
    url='https://github.com/praekelt/unique-code-service',
    license='MIT',
    description="A RESTful service for managing unique codes.",
    long_description=open('README.rst', 'r').read(),
    author='Praekelt Foundation',
    author_email='dev@praekeltfoundation.org',
    packages=["unique_code_service"],
    install_requires=[
        "Twisted", "klein", "sqlalchemy", "alchimia>=0.4", "aludel==0.2",
    ],
)
