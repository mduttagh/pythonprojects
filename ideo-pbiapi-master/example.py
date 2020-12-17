#!/usr/bin/env python

from pbiapi import PBIAPIClient

pbiapi = PBIAPIClient()

# Get all client data
client_data = pbiapi.get_client_data()

# Get all opportunity data
opportunity_data = pbiapi.get_opportunity_data()

# Get opportunities from 2020/01/01 to 2020/06/01
import datetime as dt
opportunities_data = pbiapi.get_opportunities_data(from_date=dt.date(2020, 1, 1), to_date=dt.date(2020, 6, 1))
