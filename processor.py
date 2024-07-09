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
import Pgs_connector



class processor():

    # Read the data from the BCS database
    def read_data(self):

        df = BCS_connector_blue.reader_df()

        #df = BCS_connector_purple.reader_df()


        return df
    

    # Initiates columns
    def column_initiator(self, df):

        df["Discrepancy_types"] = ""
        

        return df


    # Checks the criteria given
    def checker(self, df):

        discrepancy_types = []

        for index, row in df.iterrows():

            cost = df.loc[index, "supplier_cost"]
            listp = df.loc[index, "supplier_list"]
            p1 = df.loc[index, "p1"]


            if df.loc[index, "clean_sup_part_no"] != df.loc[index, "clean_item"]:
                discrepancy_types.append("SPN & itemid")
            
            if df.loc[index, "prod_groups"] != "BCS inv":
                discrepancy_types.append("product group")

            if df.loc[index, "buyable_locs"] != 18:
                discrepancy_types.append("Buyable locations")

            if df.loc[index, "sellable_locs"] != 18:
                discrepancy_types.append("Sellable locations")

            if df.loc[index, "delete_locs"] > 0:
                discrepancy_types.append("Delete locations")

            if df.loc[index, "discontinued_locs"] > 0:
                discrepancy_types.append("Discontinued locations")

            if df.loc[index, "purch_disc_grps"] == "DEFAULT":
                discrepancy_types.append("Product disc group") # question : should it include Default with others, or should it be only default

            if df.loc[index, "sales_disc_grps"] == "NPBSINV":
                discrepancy_types.append("Sales disc group")

            if df.loc[index, "restricted_class"] != np.nan:
                discrepancy_types.append("Restricted class")

            if df.loc[index, "std_cost_update_amt"] != 0:
                if df.loc[index, "std_cost_updates"] <= 0:
                    discrepancy_types.append("Standard cost")

            if df.loc[index, "product type"] != "Regular":
                discrepancy_types.append("Product type")

            p1_cal = round((cost / 0.65) * 2, 2)
            p1_com = 0

            if p1_cal < round(listp, 2):
                p1_com = listp
            else:
                p1_com = round((cost / 0.65) * 2, 2)

            
            if p1 != p1_com:
                discrepancy_types.append("P1")

            if df.loc[index, "restricted"] != "N":
                discrepancy_types.append("Restricted") 


            if df.loc[index, "supplier_cost"] == 0:
                discrepancy_types.append("Cost")

            if df.loc[index, "suppier_list"] == 0:
                discrepancy_types.append("list price")   # question: should it be zero or not zero?


            if df.loc[index, "shortcode"] != df.loc[index, "clean_sup_part_no"]:     # needs to be included in query
                discrepancy_types.append("shortcode & SPN")       # question: shortcode is a part of SPN?
 
        return df
    

    def main():

        pass
        
        # it should return the df
        # return df