# xmltotabular

[![PyPI](https://img.shields.io/pypi/v/xmltotabular.svg)](https://pypi.org/project/xmltotabular/)
[![License](https://img.shields.io/github/license/simonwiles/xmltotabular)](https://github.com/simonwiles/xmltotabular/blob/main/LICENSE)
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

- Tests

  ```
  $ pipenv run pytest
  ```

- Coverage

  To collect coverage execution data, use:

  ```
  $ pipenv run coverage run -m pytest
  ```

  and to get a report on the data, use:

  ```
  $ pipenv run coverage report -m
  ```

  or

  ```
  $ pipenv run coverage html
  ```

  to create an HTML report in `htmlcov/`.
