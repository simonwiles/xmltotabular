# xmltotabular

[![PyPI](https://img.shields.io/pypi/v/xmltotabular.svg)](https://pypi.org/project/xmltotabular/)
[![License](https://img.shields.io/github/license/simonwiles/xmltotabular)](https://github.com/simonw/sqlite-utils/blob/main/LICENSE)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

Python library for converting XML to tabular data.

## Development

With a working version of Python >= 3.6 and Pipenv:

1. Install dependencies.  
   _(note that a `Pipfile.lock` is not included in this repository -- this library should work with any dependency versions which satisfy what is listed in the `Pipfile` and `setup.py`, and any necessary pinning should be specified in both)_

   ```
   $ pipenv install --dev
   ```

2. Install pre-commit hooks.
   ```
   $ pipenv run pre-commit install
   ```

### Testing

- Linting and formatting.

  ```
  $ pipenv run pre-commit run --all-files
  ```
