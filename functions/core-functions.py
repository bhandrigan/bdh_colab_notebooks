
import subprocess
import sys
import importlib
import google.cloud.bigquery
import google.cloud.storage
import google.oauth2
from google.cloud import bigquery, storage
from google.colab import auth, userdata
from google.oauth2 import service_account
import google.cloud.exceptions
from google.cloud.exceptions import NotFound
import pandas as pd
import numpy as np
import pandas_gbq
import datetime
from datetime import datetime, timedelta
import os
import json
import re
import uuid
import csv
import calendar
from calendar import week
import time
import random
import requests
import uuid
import simple_salesforce
from simple_salesforce import Salesforce, SalesforceMalformedRequest
import xlsxwriter

import yaml

def load_config(file_path):
    with open(file_path, 'r') as file:
        config = yaml.safe_load(file)
    return config


config = load_config('~/keys/project-keys/colab-settings.yaml')
print(config)
# test_config = read_config('~/keys/project-keys/colab-settings.yaml')


# Function to install and import modules and sub-modules
# def install_and_import(module_name, package_name=None, names=None, alias=None):
#     """
#     Attempts to import the module or specific names from the module.
#     If the module is not installed, it installs the package and then imports.

#     Parameters:
#     - module_name (str): The module to import.
#     - package_name (str): The pip package name to install if the module is not found.
#                           If None, assumes package name is the same as module name.
#     - names (list): List of specific names to import from the module.
#                     If None, imports the module itself.
#     - alias (str): Alias to assign to the imported module or name.
#     """
#     try:
#         if names:
#             module = importlib.import_module(module_name)
#             for name in names:
#                 imported_name = getattr(module, name)
#                 if alias and len(names) == 1:
#                     sys.modules[__name__].__dict__[alias] = imported_name
#                 else:
#                     sys.modules[__name__].__dict__[name] = imported_name
#             print(f"Imported {names} from {module_name}")
#         else:
#             imported_module = importlib.import_module(module_name)
#             if alias:
#                 sys.modules[__name__].__dict__[alias] = imported_module
#             else:
#                 sys.modules[__name__].__dict__[module_name] = imported_module
#             print(f"Imported {module_name}")
#     except ImportError:
#         if package_name is None:
#             package_name = module_name.replace('.', '-')
#         print(f"Installing package {package_name}")
#         subprocess.check_call([sys.executable, "-m", "pip", "install", package_name])
#         # Try importing again
#         try:
#             if names:
#                 module = importlib.import_module(module_name)
#                 for name in names:
#                     imported_name = getattr(module, name)
#                     if alias and len(names) == 1:
#                         sys.modules[__name__].__dict__[alias] = imported_name
#                     else:
#                         sys.modules[__name__].__dict__[name] = imported_name
#                 print(f"Installed {package_name} and imported {names} from {module_name}")
#             else:
#                 imported_module = importlib.import_module(module_name)
#                 if alias:
#                     sys.modules[__name__].__dict__[alias] = imported_module
#                 else:
#                     sys.modules[__name__].__dict__[module_name] = imported_module
#                 print(f"Installed {package_name} and imported {module_name}")
#         except ImportError as e:
#             print(f"Failed to import {module_name} after installing {package_name}: {e}")

# # Mapping of module names to pip package names
# module_to_pip = {
#     "google.cloud.bigquery": "google-cloud-bigquery",
#     "pandas_gbq": "pandas-gbq",
#     "simple_salesforce": "simple-salesforce",
#     "google.colab": "google-colab",
#     "google.oauth2": "google-auth",
#     "google.cloud.storage": "google-cloud-storage",
#     "google.cloud.exceptions": "google-cloud-exceptions",
#     "requests": "requests",
#     "xlsxwriter": "XlsxWriter",
#     "pandas": "pandas",
#     "numpy": "numpy",
#     "uuid": "uuid",
#     "re": None,
#     "os": None,
#     "json": None,
#     "datetime": None,
#     "time": None,
#     "random": None,
#     "csv": None,
#     "calendar": None,
# }

# # List of modules to check and import
# modules_to_import = [
#     {"module_name": "google.cloud", "names": ["bigquery", "storage"]},
#     {"module_name": "pandas_gbq", "names": None},
#     {"module_name": "simple_salesforce", "names": ["Salesforce", "SalesforceMalformedRequest"]},
#     {"module_name": "google.colab", "names": ["auth"]},
#     {"module_name": "google.oauth2", "names": ["service_account"]},
#     {"module_name": "google.cloud.storage", "names": None},
#     {"module_name": "google.cloud.exceptions", "names": ["NotFound"]},
#     {"module_name": "requests", "names": None},
#     {"module_name": "xlsxwriter", "names": None},
#     {"module_name": "pandas", "alias": "pd", "names": None},
#     {"module_name": "numpy", "alias": "np", "names": None},
#     {"module_name": "datetime", "names": ["datetime", "timedelta"]},
#     {"module_name": "os", "names": None},
#     {"module_name": "json", "names": None},
#     {"module_name": "re", "names": None},
#     {"module_name": "uuid", "names": None},
#     {"module_name": "csv", "names": None},
#     {"module_name": "time", "names": None},
#     {"module_name": "random", "names": None},
#     {"module_name": "calendar", "names": None},
#     {"module_name": "calendar", "names": ['week']},
# ]

# # Install and import modules
# for module_info in modules_to_import:
#     module_name = module_info.get('module_name')
#     package_name = module_to_pip.get(module_name)
#     names = module_info.get('names')
#     alias = module_info.get('alias')
#     install_and_import(module_name, package_name, names, alias)


print('Imported [\'userdata\'] from google.colab')
# Now you can use the imported modules and names directly
# For example, pandas is available as 'pd', numpy as 'np', etc.

# Example usage:
# df = pd.DataFrame()
# array = np.array([1, 2, 3])