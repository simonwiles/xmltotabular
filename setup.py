from setuptools import setup, find_packages

VERSION = "0.1"

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name="xmltotabular",
    description="Covert XML to tabular data according to YAML configuration",
    long_description=long_description,
    long_description_content_type="text/markdown",
    version=VERSION,
    author="Simon Wiles",
    url="https://github.com/simonwiles/xmltotabular",
    packages=find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.6",
)
