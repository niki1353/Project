import pandas as pd
f = pd.read_csv("employee_data.csv", encoding='ISO-8859-1')
for i in f.columns:
    print(i,"=",f[f"{i}"].isnull().sum())