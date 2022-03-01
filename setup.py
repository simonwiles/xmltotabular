from setuptools import setup, find_packages

version = {}
with open("xmltotabular/_version.py", "r") as _fh:
    exec(_fh.read(), version)

with open("README.md", "r") as _fh:
    long_description = _fh.read()

setup(
    name="xmltotabular",
    description="Covert XML to tabular data according to YAML configuration",
    long_description=long_description,
    long_description_content_type="text/markdown",
    version=version["__version__"],
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
        "Programming Language :: Python :: 3.10",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Development Status :: 3 - Alpha",
    ],
    python_requires=">=3.6",
)
