#!/usr/bin/env python

"""The setup script."""

from setuptools import setup, find_packages

with open("README.rst") as readme_file:
    readme = readme_file.read()

with open("HISTORY.rst") as history_file:
    history = history_file.read()

requirements = []

test_requirements = [
    "pytest>=3",
]

setup(
    author="Vinod Ronold Aravind Jose",
    author_email="vinodronold@gmail.com",
    python_requires=">=3.6",
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Natural Language :: English",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
    ],
    description="Oracle Cloud Application Integration Package",
    install_requires=requirements,
    license="MIT license",
    long_description=readme + "\n\n" + history,
    include_package_data=True,
    keywords="pyoracloud",
    name="pyoracloud",
    packages=find_packages(include=["pyoracloud", "pyoracloud.*"]),
    test_suite="tests",
    tests_require=test_requirements,
    url="https://github.com/DataViewer-Cloud/pyoracloud",
    version="0.1.0",
    zip_safe=False,
)
