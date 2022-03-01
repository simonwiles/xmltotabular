# xmltotabular

[![PyPI](https://img.shields.io/pypi/v/xmltotabular?logo=pypi&logoColor=white)](https://pypi.org/project/xmltotabular/)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/xmltotabular?logo=python&logoColor=white)](https://pypi.org/project/xmltotabular/)
[![Tests](https://github.com/simonwiles/xmltotabular/actions/workflows/test.yml/badge.svg)](https://github.com/simonwiles/xmltotabular/actions/workflows/test.yml)
[![License](https://img.shields.io/github/license/simonwiles/xmltotabular)](https://github.com/simonwiles/xmltotabular/blob/main/LICENSE)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

Python library for converting XML to tabular data.


## Current Status

This library is under periodic development. It is useful as it stands (see [sul-cidr/patent_data_extractor](https://github.com/sul-cidr/patent_data_extractor) for the _de facto_ reference implementation), but there is still much to be done before a `1.0` release. Please get in touch if this project could be useful to you, and especially if you'd be interesting in contributing (I would welcome help with documentation and examples for a robust test suite, for example).

<!-- markdownlint-disable commands-show-output -->
## Development

With a working version of Python >= 3.6 and Pipenv:

1. Install dependencies.  
   _(note that a `Pipfile.lock` is not included in this repository -- this library should work with any dependency versions which satisfy what is listed in the `Pipfile` and `setup.py`, and any necessary pinning should be specified in both)_

   ```sh
   $ pipenv install --dev
   ```

2. Install pre-commit hooks.

   ```sh
   $ pipenv run pre-commit install
   ```


### Testing

- Linting and formatting.

  ```sh
  $ pipenv run pre-commit run --all-files
  ```

- Tests

  ```sh
  $ pipenv run pytest
  ```

- Coverage

  To collect coverage execution data, use:

  ```sh
  $ pipenv run coverage run -m pytest
  ```

  and to get a report on the data, use:

  ```sg
  $ pipenv run coverage report -m
  ```

  or

  ```sh
  $ pipenv run coverage html
  ```

  to create an HTML report in `htmlcov/`.
