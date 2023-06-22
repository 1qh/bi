import pandas as pd

df = pd.read_csv('b2c/rfm.csv')

df['R'] = pd.qcut(
    df['recency'].rank(method='first'),
    5,
    labels=[5, 4, 3, 2, 1],
)
df['F'] = pd.qcut(
    df['frequency'],
    5,
    labels=[1, 2, 3, 4, 5],
)
df['M'] = pd.qcut(
    df['monetary'],
    5,
    labels=[1, 2, 3, 4, 5],
)
df['RFM'] = df['R'].astype(str) + df['F'].astype(str)
seg_map = {
    r'[1-2][1-2]': 'hibernating',
    r'[1-2][3-4]': 'at_Risk',
    r'[1-2]5': 'cant_loose',
    r'3[1-2]': 'about_to_sleep',
    r'33': 'need_attention',
    r'[3-4][4-5]': 'loyal_customers',
    r'41': 'promising',
    r'51': 'new_customers',
    r'[4-5][2-3]': 'potential_loyalists',
    r'5[4-5]': 'champions',
}
df['segment'] = df['RFM'].replace(seg_map, regex=True)

df.to_csv('b2c/segment.csv', index=False)

seg = (
    df.groupby('segment')
    .size()
    .reset_index(name='count')
    .sort_values(by=['count'])
    .reset_index(drop=True)
)
seg.to_csv('b2c/segment_count.csv', index=False)
