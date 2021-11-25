import pandas as pd
import numpy as np
from natsort import index_natsorted
import sys

path = sys.argv[1]

courses = pd.read_csv(path)
courses.sort_values(by="grade", inplace=True)
courses.sort_values(by="course_name", key=lambda x: np.argsort(index_natsorted(courses["course_name"])), inplace=True)
courses.sort_values(by="short_name", key=lambda x: np.argsort(index_natsorted(courses["short_name"])), inplace=True)
courses.to_csv(path, index=False)