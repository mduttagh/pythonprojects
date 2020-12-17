import os
import requests
import datetime as dt

from .endpoints import Endpoint


class PBIAPIClient(object):
    BASE_URL = "https://apim-bimodel-prod.azure-api.net/powerbi-model/v1/"

    # ENDPOINTS defines the configuration for each PowerBI API endpoint
    ENDPOINTS = {
        "client": {
            "api_name": "client",
            "address": "shared/client",
            "mandatory_params": {
                "PageNo": int
            },
            "optional_params": {
                "LastUpdatedDate": dt.date,
                "LatestDataOnly": bool
            }
        },
        "opportunity": {
            "api_name": "opportunity",
            "address": "shared/opportunity",
            "mandatory_params": {
                "PageNo": int
            },
            "optional_params": {
                "LastUpdatedDate": dt.date,
                "LatestDataOnly": bool
            }
        },
        "opportunities": {
            "api_name": "opportunities",
            "address": "crm/opportunities",
            "mandatory_params": {
                "PageNo": int,
                "ReportCurrencyCode": str,
                "FromDate": dt.date,
                "ToDate": dt.date
            },
            "optional_params": {
                "LastUpdatedDate": dt.date,
                "LatestDataOnly": bool,
                "OpportunityID": int,
                "ManagingStudioName": str
            }
        },
        "person": {
            "api_name": "person",
            "address": "shared/person",
            "mandatory_params": {
                "PageNo": int
            },
            "optional_params": {
                "LastUpdatedDate": dt.date,
                "LatestDataOnly": bool
            }
        },
        "people": {
            "api_name": "people",
            "address": "shared/people",
            "mandatory_params": {
                "PageNo": int
            },
            "optional_params": {
                "LastUpdatedDate": dt.date,
                "LatestDataOnly": bool
            }
        },
    }

    def __init__(self):
        if "OKTA_USERNAME" not in os.environ:
            raise EnvironmentError(
                "OKTA_USERNAME (e.g. name@ideo.com) not set in environment.")
        self.username = os.environ["OKTA_USERNAME"]

        if "OKTA_PASSWORD" not in os.environ:
            raise EnvironmentError("OKTA_PASSWORD not set in environment.")
        self.password = os.environ["OKTA_PASSWORD"]

        if "PBIAPI_SUBSCRIPTION_KEY" not in os.environ:
            raise EnvironmentError(
                "PBIAPI_SUBSCRIPTION_KEY not set in environment.")
        self.subscription_key = os.environ["PBIAPI_SUBSCRIPTION_KEY"]

        self.headers = {"Content-Type": "application/json",
                        "Ocp-Apim-Subscription-Key": self.subscription_key}

        self.token = self.get_okta_token()
        self.headers.update({"Authorization": self.token})

        self.last_generated = dt.datetime.now()
        # TODO: Regenerate token after expiration period

    def get_okta_token(self):
        """Get Okta token based on ideo userid/password"""
        creds = {"username": self.username, "password": self.password}
        token_endpoint = self.BASE_URL + "auth/getapitoken"
        try:
            response = requests.post(
                token_endpoint, headers=self.headers, json=creds)
            response.raise_for_status()
        except requests.exceptions.RequestException as err:
            print("Failed to get token(check that your username, password,"
                  "and subscription key are correct in your .env file): \n", str(err))
        else:
            token = response.text
            return token

    def _get_data_default(self, endpoint_name: str,
                          start_page: int = 1,
                          last_updated: dt.date = None,
                          latest_only: bool = None):
        endpoint_config = self.ENDPOINTS[endpoint_name]
        endpoint = Endpoint(endpoint_config, self.BASE_URL, self.headers)
        params = {"PageNo": start_page,
                  "LastUpdatedDate": last_updated,
                  "LatestDataOnly": latest_only}
        return endpoint.get(params)

    def get_client_data(self,
                        start_page: int = 1,
                        last_updated: dt.date = None,
                        latest_only: bool = None):
        return self._get_data_default("client",
                                      start_page=start_page,
                                      last_updated=last_updated,
                                      latest_only=latest_only)

    def get_opportunity_data(self,
                             start_page: int = 1,
                             last_updated: dt.date = None,
                             latest_only: bool = None):
        return self._get_data_default("opportunity",
                                      start_page=start_page,
                                      last_updated=last_updated,
                                      latest_only=latest_only)

    def get_opportunities_data(self,
                               from_date: dt.date,
                               to_date: dt.date,
                               start_page: int = 1,
                               currency_code: str = "USD",
                               last_updated: dt.date = None,
                               latest_only: bool = None,
                               managing_studio: str = None,
                               opportunity_id: int = None):
        endpoint_config = self.ENDPOINTS["opportunities"]
        endpoint = Endpoint(endpoint_config, self.BASE_URL, self.headers)
        params = {"PageNo": start_page,
                  "ReportCurrencyCode": currency_code,
                  "FromDate": from_date,
                  "ToDate": to_date,
                  "LastUpdatedDate": last_updated,
                  "OpportunityID": opportunity_id,
                  "LatestDataOnly": latest_only,
                  "ManagingStudioName": managing_studio}
        endpoint.check_params(params)
        assert to_date > from_date
        assert to_date - from_date < dt.timedelta(days=366)
        return endpoint.get(params)

    def get_person_data(self,
                        start_page: int = 1,
                        last_updated: dt.date = None,
                        latest_only: bool = None):
        return self._get_data_default("person",
                                      start_page=start_page,
                                      last_updated=last_updated,
                                      latest_only=latest_only)

    def get_people_data(self,
                        start_page: int = 1,
                        last_updated: dt.date = None,
                        latest_only: bool = None):
        return self._get_data_default("people",
                                      start_page=start_page,
                                      last_updated=last_updated,
                                      latest_only=latest_only)


if __name__ == "__main__":
    client = PBIAPIClient()
    print(client.token)
    client.get_client_data()
    # client.get_opportunity_data(last_updated=dt.date(2020, 1, 1))
    # client.get_opportunities_data(from_date=dt.date(
    # 2020, 1, 1), to_date=dt.date(2020, 6, 1))
    # client.get_person_data()
    # client.get_people_data()
