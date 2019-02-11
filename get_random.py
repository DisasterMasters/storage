import contextlib
import itertools
import pickle

import pandas as pd

from common import *

COLLNAMES = [
    "Statuses_Irma_A",
    "Statuses_Maria_A",
    "Statuses_Florence_A"
]

if __name__ == "__main__":
    try:
        with open("pandas.pkl", "rb") as fd:
            df = pickle.load(fd)
    except FileNotFoundError:
        with contextlib.ExitStack() as exitstack:
            conn = exitstack.enter_context(openconn())
            records = list(itertools.chain.from_iterable(exitstack.enter_context(conn["twitter"][collname].find(no_cursor_timeout = True)) for collname in COLLNAMES))

        df = pd.DataFrame(records)

        with open("pandas.pkl", "wb") as fd:
            pickle.dump(df, fd)

    print(df.sample(500))

