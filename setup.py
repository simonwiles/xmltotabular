from setuptools import setup, find_packages

VERSION = "0.6"

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name="xmltotabular",
    description="Covert XML to tabular data according to YAML configuration",
    long_description=long_description,
    long_description_content_type="text/markdown",
    version=VERSION,
    author="Simon Wiles",
    author_email="simonjwiles@gmail.com",
    url="https://github.com/simonwiles/xmltotabular",
    packages=find_packages(),
    install_requires=[
        "lxml",
        "pyyaml",
        "termcolor",
        "multiprocess ; python_version < '3.7'",
    ],
    classifiers=[
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Development Status :: 3 - Alpha",
    ],
    python_requires=">=3.6",
)
