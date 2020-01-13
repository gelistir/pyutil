import os
import pathlib
import pandas as pd


def folder_stamped(*args):
    folder = os.path.join(os.path.join(*args), "{t}".format(t=pd.Timestamp("today").strftime("%Y%m%d")))
    pathlib.Path(folder).mkdir(parents=True, exist_ok=True)
    return folder
