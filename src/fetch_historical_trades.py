import datetime as dt
import json
import time
import urllib.request
from typing import IO, List
import argparse
import os
from time import sleep

parser = argparse.ArgumentParser(description='Download all aggregated trades data. Starts from the latest in the output file.')
parser.add_argument('--pair', '-p', type=str, required=True,
                    help='The pair for which to fetch data. Ex: ETHUSDT')
parser.add_argument('--output', '-o', type=str, required=True,
                    help='Output file.')

args = parser.parse_args()


# Gets the aggregated trades from binance, from id 'start_id' for a pair 'pair'
# and saves each record in a csv file located at 'save_file_path'
# CSV contains data: price, volume, time, id


# Api request configuration
# ================

pair = args.pair
save_file_path: str = args.output

start_id = 0

if (os.path.exists(save_file_path)):
    with open(save_file_path, 'r') as file:
        file.seek(0, os.SEEK_END)
        file.seek(file.tell() - 2, os.SEEK_SET)

        while file.read(1) != '\n': # go back until new line is found
            file.seek(file.tell() - 2, os.SEEK_SET)

        lastline = file.readline()
        print(lastline)
        start_id = int(lastline.split(',')[-1]) + 1

        print(f"Will start fetching from ID = {start_id}")
        sleep(10)




# Api client configuration
# ================

trades_api_url = "https://api.binance.com/api/v3/aggTrades"
api_error_waiting_time = 60 # Time to wait in case there a request fails.
throtle = 0.12 # minimum time between 2 requests.



# Utility functions and classes
# ================

class OHLCRecord:
    def __init__(self, price, volume, time, id):
        self.price = price
        self.volume = volume
        self.time = time
        self.id = id


def buildUrl(pair: str, idx:int) -> str:
    return trades_api_url + "?limit=1000&symbol=" + pair + "&fromId=" + str(idx)


def toCSV(record: OHLCRecord) -> str:
    """
    Transforms a record to a csv line
    "price, volume, time, id"
    :param record:
    :return:
    """
    return f"{record.price},{record.volume},{record.time},{record.id}\n"


# Process the response and append data to a file
def append_to_file(ohlc_record: OHLCRecord, file: IO):
    line = toCSV(ohlc_record)
    file.write(line)


def process_api_response(response: str) -> int:
    """
    :param response:
    :return: The id of the last aggregated trade.
    """
    results: List = json.loads(response)
    try:
        if len(results) == 0:
            return -2

        # results is a dictionary where the key is the pair and the value is an array of array of our expected data

        data_until_date = dt.datetime.utcfromtimestamp(results[-1]["T"] / 1000)
        print(f"Got data up to {data_until_date.strftime('%Y-%m-%d %H:%M:%S')}")

        with open(save_file_path, mode='a') as file:
            for ohlc_data in results:
                record = OHLCRecord(price=ohlc_data["p"],
                               volume=ohlc_data["q"],
                               time=ohlc_data["T"] / 1000,
                               id=ohlc_data["a"]
                               )
                append_to_file(record, file)

        return results[-1]["a"]

    except:
        print(response)
        time.sleep(30)

        return -1


# ================
# Main
# ================

idx = start_id

while idx != None:
    # We do a request, get 1000 aggregated trades
    # for the next request, we increment the last trade id by 1.

    url = buildUrl(pair, idx)

    with urllib.request.urlopen(url) as response:
        response_data = response.read()

        new_idx = process_api_response(response_data)

        if new_idx == -1:
            print(f"The server throttled our calls.")
        if new_idx == -2:
            print("We got the last data.")
        else:
            idx = new_idx + 1

        time.sleep(throtle)


