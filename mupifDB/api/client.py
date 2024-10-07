import requests
import json
import datetime
import sys
import os
import tempfile
import importlib
import re
import logging

import pydantic
from typing import List,Optional,Literal

from .. import models
from .. import table_structures

from rich import print_json
from rich.pretty import pprint

from .client_util import api_type

if api_type=='granta':
    from .client_granta import *
else:
    from .client_mupif import *

from .client_edm import *
