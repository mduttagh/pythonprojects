# ideo-pbiapi

Welcome to IDEO's Python client for our [PowerBI API](https://apim-bimodel-prod.developer.azure-api.net/api-details#api=powerbi-model-v1).

## Installation

Pipenv

1. `pipenv install`
2. Request a PowerBI API subscription key from [the ERP team by email](mailto:erp@ideo.com)
3. `cp .env.default .env`
4. Fill in your Okta credentials + PowerBI API subscription key in the newly created `.env` file

## Usage

```sh
pipenv run ./example.py
```

## Results

Data are returned as records, e.g. `[ [{"col1": val1, "col2": val2, ...}], [...], ...]`

## Development Notes

* If you add any new dependencies to the `Pipfile`, be sure to run `pipenv run pipenv-setup sync` to add those dependencies to the setup.py.
