# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.16.1
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %% jupyter={"outputs_hidden": true}
# !pip3 install pandas

# %% jupyter={"outputs_hidden": true}
# !pip3 install openpyxl

# %%
import pandas as pd

# %%
df = pd.read_excel('/root/12_04_08_E_CCTV.xlsx')

# %%
df.to_csv('/root/hadoop-3.3.6/CCTV.csv',encoding='utf-8', header='true')

# %%
df

# %%
df.isnull().sum()

# %%
key_value =  df.groupby(['관리기관명'])['번호'].count()

# %%
#세션2
key_value_dict = dict(key_value)
sorted(key_value_dict.items(),key = lambda x : x[1], reverse=True)

# %%
#세션3 
#관리기관, 설치목적 별로 정렬헤서 건수 발견
df1 = df.groupby(['관리기관명','설치목적구분'])['번호'].count()

# %%
df1
key_value_dict = dict(df1)
sorted(key_value_dict.items(),key = lambda x : x[0], reverse=True)

# %%
