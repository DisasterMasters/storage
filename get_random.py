import contextlib
import itertools
import pickle
import sys

import pandas as pd

from common import *

COLLNAMES = [
    "Statuses_Irma_A",
    "Statuses_Maria_A",
    "Statuses_Florence_A"
]

SAMPLE_SIZE = 500

if __name__ == "__main__":
    sample = []
    
    with openconn() as conn:
        for collname in COLLNAMES:
            coll = conn["twitter"][collname]

            sample.extend(coll.aggregate([{"$sample": {"size": (SAMPLE_SIZE // len(COLLNAMES) + 1)}}]))

    df = pd.DataFrame(sample)

    if len(sys.argv) > 1:
        with open(sys.argv[1], "w") as fd:
            df.to_csv(fd, index = False)
    else:
        df.to_csv(sys.stdout, index = False)
      
