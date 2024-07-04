# importing libraries
import pandas as pd
import numpy as np
import re
import argparse
import sys
import os
import glob
import logging
from datetime import datetime
import json
import BCS_connector_blue
import BCS_connector_purple
import Pgs_connector



class processor():

    # Read the data from the BCS database
    def read_data(self, segment_color):

        if segment_color.lower() == "blue": 
            # get the df and the connection
            df = BCS_connector_blue.reader_df()

        if segment_color.lower() == "purple":
            # get the df and the connnection
            df = BCS_connector_purple.reader_df()


        return df
    

    # Checks the criteria given
    def checker(self, df):


        return df