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


    def modifier(self, df):

        df["supplier_cost"] = df["supplier_cost"].round(2)
        df["supplier_list"] = df["supplier_list"].round(2)
        df["p1"] = df["p1"].round(2)
        df["std_cost_update_amt"] = df["std_cost_update_amt"].round(2)
        df["max_mac"] = df["max_mac"].round(2)

        return df
    

    # Checks the criteria given
    def checker(self, df):


        for index, row in df.iterrows():
            discrepancy_types = []


            discrepancy_flag = 0

            #cost = round(df.loc[index, "supplier_cost"],2)
            listp = round(df.loc[index, "supplier_list"],2)
            p1 = round(df.loc[index, "p1"],2)

            #cost = round(cost, 2)
            listp =round(listp, 2)
            p1 = int(round(p1, 2))


            if df.loc[index, "clean_sup_part_no"] != df.loc[index, "clean_item"]:
                discrepancy_types.append("Clean SPN & clean itemid")
                discrepancy_flag = 1
            
            if df.loc[index, "prod_grps"] != "BCS INV":
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

            if df.loc[index, "purch_disc_grps"].strip() != "DEFAULT":
                discrepancy_types.append("Product disc group") # question : should it include Default with others, or should it be only default
                discrepancy_flag = 1

            if df.loc[index, "sales_disc_grps"].strip() != "NPBSINV":
                discrepancy_types.append("Sales disc group")
                discrepancy_flag = 1

            """if df.loc[index, "restricted_class"] != np.nan:
                discrepancy_types.append("Restricted class")
                discrepancy_flag = 1
            """
            if df.loc[index, "std_cost_update_amt"] != 0:
                if df.loc[index, "std_cost_updates"] <= 0:
                    discrepancy_types.append("Standard cost locations")
                    discrepancy_flag = 1

            if df.loc[index, "product_type"] != "Regular":
                discrepancy_types.append("Product type")
                discrepancy_flag = 1

            if round(df.loc[index, "max_mac"],2) != 0:
                cost = round(df.loc[index, "std_cost_update_amt"],2)
                p1_cal = int(round((cost / 0.65) * 2, 2))
                p1_com = 0

                if p1_cal < round(listp, 2):
                    p1_com = listp
                else:
                    p1_com = int(round((cost / 0.65) * 2, 2))

                
                if p1 != p1_com:
                    diff = p1 - p1_com
                    tolerance = 0.2

                    if abs(diff)>= tolerance:

                        discrepancy_types.append("P1")
                        discrepancy_flag = 1

            if df.loc[index, "restricted"] != "N":
                discrepancy_types.append("Restricted")
                discrepancy_flag = 1 


            if df.loc[index, "supplier_cost"] != 0:
                discrepancy_types.append("Supplier Cost")
                discrepancy_flag = 1

            if df.loc[index, "supplier_list"] != 0:
                discrepancy_types.append("list price")   
                discrepancy_flag = 1

            
            shortcode = df.loc[index, "short_code"]

            

            if pd.notnull(shortcode):
                
                if pd.notnull(df.loc[index, "supplier_part_no"]):
                    if len(df.loc[index, "supplier_part_no"]) > 29:
                        
                        if df.loc[index, "short_code"] != df.loc[index, "supplier_part_no"][0:30]:
                            discrepancy_types.append("Shortcode & SPN")
                            discrepancy_flag = 1

                    else:
                        if df.loc[index, "short_code"] != df.loc[index, "supplier_part_no"]:
                            discrepancy_types.append("Shortcode & SPN")
                            discrepancy_flag = 1                    


            if pd.isnull(shortcode) :
                discrepancy_types.append("empty shortcode")
                discrepancy_flag = 1

            if round(df.loc[index, "std_cost_update_amt"],2) != round(df.loc[index, "max_mac"],2):
                discrepancy_types.append("Standard cost")
                discrepancy_flag = 1
 

            # assinging values to the column
            if discrepancy_flag != 1:
                df.loc[index, "discrepancy_types"] = "All right"

            elif discrepancy_flag == 1:
                discrepancy_types.sort()
                joined_discrepany = " - ".join(discrepancy_types)

                df.loc[index, "discrepancy_types"] = joined_discrepany


        return df
    

    def main(self):
        
        processorob = processor()
        df = processorob.read_data()
        df = processorob.column_initiator(df)
        df = processorob.modifier(df)
        df = processorob.checker(df)

        return df
        
        # it should return the df
        # return df