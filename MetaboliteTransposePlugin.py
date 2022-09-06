# Objective:
#   The purpose of the script is to transpose metabolomics data to feed into PLUMA pipeline
#       input:  columns are samples, rows are compound IDs
#       output: columns are compound IDs, rows are samples
#

import pandas as pd
import numpy as np


class MetaboliteTransposePlugin:
    def input(self, infile):
        self.inputfile = infile

    def run(self):
        pass

    def output(self, outputfile):
       df = pd.read_csv(self.inputfile, dtype=str)
       df = df[~df["COMP ID"].isnull()]

       # Normalize:
       columns = list(df.columns)
       for col in columns:
           if col!="COMP ID":
               df[col] = df[col].apply(lambda x: str(x).replace(",", ""))
               df[col] = df[col].astype(float)
               sumCounts = df[col].sum()
               df[col] = df[col].apply(lambda x: x/sumCounts)


       df = pd.pivot_table(df, index="COMP ID", aggfunc=np.sum)


       df = df.transpose()

       df.to_csv(outputfile)
