#!/usr/bin/env python3
#this is a pretty standard boilerplate setup script for pip
import setuptools

with open('README.md','r') as f:
    long_description = f.read()

setuptools.setup(
    name="experiment",
    version="0.1",
    author="Brian Weber",
    author_email="weberb@ornl.gov",
    description="A framework for running controlled experiments in VMware virtual environments",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://code-int.ornl.gov/navwar-challenges/experiment",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)
