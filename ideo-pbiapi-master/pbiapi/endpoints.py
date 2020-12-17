import os
import requests
import datetime as dt
from tqdm import tqdm


class Endpoint(object):
    """ Endpoint template for creating different API Endpoints.
    Endpoint is defined upon creation by the api_config """

    def __init__(self, api_config: dict, base_url: str, headers: dict):
        self.api_name = api_config['api_name']
        self.mandatory = api_config["mandatory_params"]
        self.optional = api_config["optional_params"]
        self.endpoint = base_url + api_config['address']
        self.headers = headers

    def process_params(self, params: dict):
        param_dict = {}
        for name, param in params.items():
            if param is None:
                continue
            if isinstance(param, dt.date):
                param_dict.update({name: param.strftime("(%Y,%m,%d)")})
            else:
                param_dict.update({name: param})
        return param_dict

    def check_params(self, params: dict):
        for param, param_type in self.mandatory.items():
            assert param in params
            assert isinstance(
                params[param], param_type)
        for param in params.keys():
            assert param in self.mandatory or param in self.optional

    def _get_pages(self, api_parameters: dict):
        while True:
            try:
                response = requests.post(self.endpoint,
                                         headers=self.headers,
                                         json=api_parameters)
                response.raise_for_status()
            except requests.exceptions.RequestException as err:
                print("API call failed\n", str(err))
                raise requests.exceptions.RequestException(err)
            else:
                results = response.json()
                if results["data"] is None:
                    print("Api call didn't return any data, please check parameters")
                    raise requests.exceptions.InvalidSchema(
                        "Parameters provided returned no data.")

                yield results["data"]

                # Checking end of page condition
                if results["NextPageNo"] == 0:
                    break
                else:
                    api_parameters["PageNo"] = results["NextPageNo"]

    def get(self, api_parameters: dict):
        records = []
        self.check_params(api_parameters)
        api_parameters = self.process_params(api_parameters)

        for page in tqdm(self._get_pages(api_parameters), unit="page"):
            records.extend(page)

        return records
