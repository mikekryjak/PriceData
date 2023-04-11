import numpy as np
import pandas as pd
import yfinance as yf
from forex_python.converter import CurrencyRates
import datetime as dt
import os, sys
import pickle as pkl
import traceback
import socket


from datetime import date
# from datetime import datetime
c = CurrencyRates()


class PriceData():

    # TODO: Compare against the recorded sp/sp_gbp in HL.

    def __init__(self, database_path, plays, update = True, rebuild = False, offline = False):

        self.yf_symbols = plays
        skip_plays = ["BCE.L", 
                      "MEF.F", # Has crazy SP
                      "GLV.AX", # Goes +- 100% every day due to ASX 
                      "XXX", # code for pre-ipos
                      "ACPE.L"   # delisted
                      ]
        # [self.yf_symbols.append(x) for x in ["^AXSO", "IWM", "XLE", "SPY", "BZ=F"]] NO. Do this some other way.
        
        [self.yf_symbols.remove(x) for x in skip_plays if any(x in skip_plays for x in self.yf_symbols)]

        if offline is True:
            print(">>> PriceData operating in offline mode.")
            
        if is_internet() is False:
            print(">>> Found no internet connection, switching to offline mode.")
            self.offline = True
            
            
        self.today = dt.datetime.combine(dt.date.today(), dt.datetime.min.time()).date()
        self.tomorrow = self.today + dt.timedelta(days=1)
        self.start_date = strpdate("01-01-2016")
        self.database_path = database_path
        saved_data_missing = False if "price_data.pkl" in os.listdir(self.database_path) else True
        
        if self.today.weekday() in [5,6]:
            weekend = True
        else:
            weekend = False
            
        # If database found
        if saved_data_missing == False:
            
            self.data = read_file(os.path.join(self.database_path, "price_data.pkl"))
            last_day = self.data.index[-1].date()
            print(f">>> Database found. Final entry: {last_day}")
            
            self.saved_plays = list(set(self.data.columns.get_level_values(0)))
            missing_plays = [x for x in self.yf_symbols if x not in self.saved_plays]
            
            if len(missing_plays) > 0:
                print("-- Missing plays in the database")
                rebuild = True
            
            # Update if out of date and overwrite
            if last_day < self.today and update is True and offline is False and weekend is False:
                print(f"-- Out of date. Updating")
                df_new = self.get_data(last_day + dt.timedelta(days=1), self.tomorrow)
                if last_day == df_new.index[-1].date():
                    print("---> No new data available")
                else:
                    self.data = pd.concat([self.data, df_new])
                    print(f"--> Updated. New final entry: {self.data.index[-1].date()}")
                    write_file(self.data, os.path.join(self.database_path, "price_data.pkl"))
                    
            elif weekend is True:
                print("--> It's a weekend, skipping updates")
        
        
        # If no database found
        if saved_data_missing == True or rebuild == True and offline is False:
            print(">>> Rebuilding database")
            self.data = self.get_data(self.start_date, self.today)
            write_file(self.data, os.path.join(self.database_path, "price_data.pkl"))
        
        elif saved_data_missing is True and offline is True:
            raise Exception("--> Database offline and file not found.")
                
                
        all_dates = pd.DataFrame(index = pd.date_range(start = self.data.index[0], end = self.data.index[-1]))
        all_dates = pd.concat([self.data, all_dates]).sort_index()
        self.missing_dates = all_dates[~all_dates.index.duplicated(keep=False)].index
        
        plays_downloaded = self.data.columns.get_level_values(0).drop_duplicates()
        # self.missing_plays = 
            

        # Fill days to make sure there is an entry for each day
        # NOTE this is probably a bad idea, and even if not 
        # it is implemented poorly. Filled dates should have 
        # OHLC values all equal to the previous close 
        # at the very least, otherwise it is literally wrong. 
        # self.data = self.fill_dates(self.data)
       
       
    def get_data(self, start_date, end_date, remove_missing = False):
        """
        Return multiindex yfinance dataframe
        Columns grouped by play
        """
        print(f"-- Downloading data from {start_date} to {end_date}...")
        df = yf.download(self.yf_symbols, start = start_date, end = end_date, interval="1d", progress = True, keepna = True, auto_adjust = True)
        df.columns = df.columns.swaplevel(0, 1)
        df.sort_index(axis = 1, level = 0, inplace = True)
        
        if remove_missing is True:
            # Drop invalid plays
            closes = df.loc(axis=1)[:, "Close"]
            closes.columns = closes.columns.droplevel(1)   # drop Close from multiindex
            nan_count = closes.isna().astype(int).sum()
            invalid_plays = list(nan_count[nan_count == len(closes)].index)   # remove if all nans

            for play in invalid_plays:
                df = df.drop(play, axis = 1, level = 0)
                print(f"Removing {play}")
        
        return df
    
    
    
    def make_old_data(self, start_date, end_date):
        """
        Test updating system by making database that is out of date
        """
        df = self.get_data(start_date, end_date)
        write_file(df, os.path.join(self.database_path, "price_data.pkl"))
        
    
    
    def fill_dates(self, df):
            # Ensure dataframe contains all days.
            all_dates = pd.DataFrame(index = pd.date_range(start = df.index[0], end = df.index[-1]))
            all_dates = pd.concat([df, all_dates]).sort_index()
            missing_dates = all_dates[~all_dates.index.duplicated(keep=False)]
            
            df = pd.concat([df, missing_dates]).sort_index().fillna(method="ffill")

            return df
        
          
          
    
    
        # if "price_data.pkl" in os.listdir(os.getcwd()) and override == False:
        #     rebuild = False
        # else:
        #     rebuild = True

        # if "price_data.pkl" in os.listdir(os.getcwd()) and overwrite == False:

        #     self.price_data = read_file("price_data.pkl")
        #     self.last_date = self.price_data.index[-1]
            
        #     days_old = (self.today - self.last_date).days
        #     if days_old == 0:
        #         print(">>> Price data up to date")
        #     else:
        #         print(f">>> Data is {days_old} days old")

        #     if days_old > 0:
        #         print("--> Updating...")

        #         try:
        #             new_data = self.download_data(self.last_date)
        #             self.price_data = pd.concat([self.price_data, new_data])
        #             self.price_data = self.fill_dates(self.price_data)
        #             write_file(self.price_data, "price_data.pkl")
        #             print("--> Complete")

        #         except Exception:
        #             print("Update failed")
        #             traceback.print_exc()


        # else:
        #     print("Price database not found, rebuilding")
        #     self.price_data = self.download_data(dt.datetime.strptime("01-01-2019", "%d-%m-%Y"))
        #     write_file(self.price_data, "price_data.pkl")

        # If the database is updated on a weekend, yfinance won't have today's data.
        # This causes a mismatch and there is no row for "today".
        # This ensures that there is a "today" and it's a copy of the last row in this instance.
        # if self.today > self.last_date:
        #     self.price_data.loc[self.today] = self.price_data.loc[self.last_date].copy()


def write_file(data, filename, quiet = True):
# Writes an object to a pickle file.
    
    with open(filename, "wb") as file:
    # Open file in write binary mode, dump result to file
        pkl.dump(data, file)
        if not quiet:
            print("{} written".format(filename))
        
        
        
def read_file(filename, quiet = True):
# Reads a pickle file and returns it.

    with open(filename, "rb") as f:
    # Open file in read binary mode, dump file to result.
        data = pkl.load(f)
        if not quiet:
            print("{} loaded".format(filename))
        
    return data


def is_internet(host="8.8.8.8", port=53, timeout=3):
    """
    Host: 8.8.8.8 (google-public-dns-a.google.com)
    OpenPort: 53/tcp
    Service: domain (DNS/TCP)
    """
    return True
    try:
        socket.setdefaulttimeout(timeout)
        socket.socket(socket.AF_INET, socket.SOCK_STREAM).connect((host, port))
        return True
    except socket.error as ex:
        print(ex)
        return False