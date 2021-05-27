import csv
from dataclasses import dataclass
from typing import List

# This script aggregate trades by time period.

TRADES_FILE = "~/jo_dev/CryptoTrader/CryptoData/trades_binance_eth-usd-14-05-2021.csv"
OUTPUT_FILE = "~/jo_dev/CryptoTrader/CryptoData/trades_binance_eth-usd-14-05-2021_ticker-from-trades_10sec-avg.csv"
AGGREGATION_PERIOD_SEC = 10


@dataclass
class Trade:
    id: int
    time: float
    price: float
    volume: float
    #
    # def __init__(self, id):
    #     self.id = id

    @classmethod
    def from_csv(cls, line: List[str]):
        # csv: price, volume, time, id
        return cls(int(line[3]), float(line[2]), float(line[0]), float(line[1]))

    def to_csv_string(self) -> str:
        return f"{round(self.price, 4)},{round(self.volume, 7)},{self.time},{self.id}\n"



with open(TRADES_FILE, "r") as file, open(OUTPUT_FILE, "w") as output_file:

    csv_reader = csv.reader(file, delimiter=",")

    read_idx = 0
    csv_write_idx = 1

    averaged_trade: Trade or None = None
    average_weight = 0


    for row in csv_reader:
        read_idx += 1

        if read_idx % 1000000 == 0:
            print(f"Processing line {read_idx}")

        row_trade = Trade.from_csv(row)
        row_trade.time = round(row_trade.time / AGGREGATION_PERIOD_SEC) * AGGREGATION_PERIOD_SEC

        if averaged_trade is not None and averaged_trade.time != row_trade.time:
            averaged_trade.id = csv_write_idx
            averaged_trade.price = averaged_trade.price / average_weight

            csv_write_idx += 1

            output_file.write(averaged_trade.to_csv_string())

            averaged_trade = row_trade
            average_weight = 1
            continue

        elif averaged_trade is not None:

            # Sum
            average_weight += 1
            averaged_trade.price += row_trade.price
            averaged_trade.volume += row_trade.volume

        else:
            average_weight = 1
            averaged_trade = row_trade
            continue