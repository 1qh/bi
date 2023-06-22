from polars import DataFrame, col, date, read_csv


def overview(df: DataFrame):
    print(df)
    print(df.describe())
    print(df.glimpse())


def export(df: DataFrame, path: str):
    df.write_csv(path)
    assert read_csv(
        path,
        use_pyarrow=True,
    ).frame_equal(df)


rfm = (
    read_csv('b2c/total_by_order.csv', use_pyarrow=True)
    .join(
        read_csv('b2c/sales.csv', use_pyarrow=True),
        on='id',
    )
    .select('customer_id', 'time', 'total')
    .groupby('customer_id', maintain_order=True)
    .agg(
        (date(2022, 5, 1) - col('time').max()).dt.days().alias('recency'),
        col('time').count().alias('frequency'),
        col('total').sum().round(2).alias('monetary'),
    )
    .sort('frequency', descending=True)
)
export(rfm, 'b2c/rfm.csv')
