from collections import namedtuple
from typing import Any
from urllib.parse import urljoin, urlencode, urlunparse
import requests
from requests.adapters import HTTPAdapter, Retry

from pyproj import CRS

from .nwb_wegen import NWBWegen
    
class PDOKClient():
    Components = namedtuple(
        typename='Components', 
        field_names=['scheme', 'netloc', 'url', 'path', 'query', 'fragment']
    )
    
    def __init__(self, 
                 crs = "OGC:CRS84" #CRS.projparam, default=API default
                 ) -> None:
        # setup session with API
        self.api_pdok_url = urlunparse(self.Components(
            scheme="https",
            netloc="api.pdok.nl",
            query='',
            path='',
            url='/',
            fragment=''))
        self.rws_url = urljoin(self.api_pdok_url, "rws/")
        self.pdok_session = requests.Session()
        retries = Retry(total=3,
                        backoff_factor=1,
                        status_forcelist=[ 500, 502])
        self.pdok_session.mount('http://', HTTPAdapter(max_retries=retries))
        
        # set crs default
        self.crs = CRS("OGC:CRS84")
        
        # setup modules, per dataset
        self.nwb_wegen = NWBWegen(pdok_client=self)