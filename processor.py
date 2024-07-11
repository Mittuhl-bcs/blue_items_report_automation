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

        df["discrepancy_types"] = ""
        

        return df


    # Checks the criteria given
    def checker(self, df):

        discrepancy_types = []

        for index, row in df.iterrows():

            discrepancy_flag = 0

            cost = df.loc[index, "supplier_cost"]
            listp = df.loc[index, "supplier_list"]
            p1 = df.loc[index, "p1"]


            if df.loc[index, "clean_sup_part_no"] != df.loc[index, "clean_item"]:
                discrepancy_types.append("SPN & itemid")
                discrepancy_flag = 1
            
            if df.loc[index, "prod_groups"] != "BCS inv":
                discrepancy_types.append("product group")
                discrepancy_flag = 1

            if df.loc[index, "buyable_locs"] != 18:
                discrepancy_types.append("Buyable locations")
                discrepancy_flag = 1

            if df.loc[index, "sellable_locs"] != 18:
                discrepancy_types.append("Sellable locations")
                discrepancy_flag = 1

            if df.loc[index, "delete_locs"] > 0:
                discrepancy_types.append("Delete locations")
                discrepancy_flag = 1

            if df.loc[index, "discontinued_locs"] > 0:
                discrepancy_types.append("Discontinued locations")
                discrepancy_flag = 1

            if df.loc[index, "purch_disc_grps"] == "DEFAULT":
                discrepancy_types.append("Product disc group") # question : should it include Default with others, or should it be only default
                discrepancy_flag = 1

            if df.loc[index, "sales_disc_grps"] == "NPBSINV":
                discrepancy_types.append("Sales disc group")
                discrepancy_flag = 1

            if df.loc[index, "restricted_class"] != np.nan:
                discrepancy_types.append("Restricted class")
                discrepancy_flag = 1

            if df.loc[index, "std_cost_update_amt"] != 0:
                if df.loc[index, "std_cost_updates"] <= 0:
                    discrepancy_types.append("Standard cost locations")
                    discrepancy_flag = 1

            if df.loc[index, "product type"] != "Regular":
                discrepancy_types.append("Product type")
                discrepancy_flag = 1

            p1_cal = round((cost / 0.65) * 2, 2)
            p1_com = 0

            if p1_cal < round(listp, 2):
                p1_com = listp
            else:
                p1_com = round((cost / 0.65) * 2, 2)

            
            if p1 != p1_com:
                discrepancy_types.append("P1")
                discrepancy_flag = 1

            if df.loc[index, "restricted"] != "N":
                discrepancy_types.append("Restricted")
                discrepancy_flag = 1 


            if df.loc[index, "supplier_cost"] != 0:
                discrepancy_types.append("Cost")
                discrepancy_flag = 1

            if df.loc[index, "suppier_list"] != 0:
                discrepancy_types.append("list price")   
                discrepancy_flag = 1

            shortcode = df.loc[index, "short_code"]
            shortcode_cleaned = re.sub(r'[^a-zA-Z0-9\s]', "", shortcode)
            if shortcode_cleaned != df.loc[index, "clean_sup_part_no"]:     # needs to be included in query
                discrepancy_types.append("shortcode & SPN")       # question: shortcode is a part of SPN?
                discrepancy_flag = 1

            if df.loc[index, "std_cost_updates"] > 0:
                discrepancy_types.append("Standard cost")
                discrepancy_flag = 1
 

            # assinging values to the column
            if discrepancy_flag != 1:
                df.loc[index, "discrepancy_types"] = "All right"

            elif discrepancy_flag == 1:
                joined_discrepany = " - ".join(discrepancy_types)

                df.loc[index, "discrepancy_types"] = joined_discrepany


        return df
    

    def main():
        
        processorob = processor()
        df = processorob.read_data()
        df = processorob.column_initiator(df)
        df = processorob.checker(df)

        return df
        
        # it should return the df
        # return df