from polars import col, date, read_csv

from utils import export

sales = read_csv('b2c/sales.csv', use_pyarrow=True)
customer = read_csv('data/customer.csv', use_pyarrow=True)
store = read_csv('data/store.csv', use_pyarrow=True)
total_by_order = read_csv('b2c/total_by_order.csv', use_pyarrow=True)

rfm = (
    total_by_order.join(
        sales,
        on='id',
    )
    .select(
        'customer_id',
        'time',
        'total',
    )
    .groupby(
        'customer_id',
        maintain_order=True,
    )
    .agg(
        (date(2022, 5, 1) - col('time').max()).dt.days().alias('recency'),
        col('time').count().alias('frequency'),
        col('total').sum().round(2).alias('monetary'),
    )
    .sort(
        'frequency',
        descending=True,
    )
)
export(rfm, 'b2c/rfm.csv')

customer_per_store = (
    customer.groupby(
        'store',
        maintain_order=True,
    )
    .count()
    .join(
        store,
        left_on='store',
        right_on='id',
    )
    .select(
        'store',
        'address',
        'latitude',
        'longitude',
        'count',
    )
)
export(customer_per_store, 'b2c/customer_per_store.csv')

order_by_date = (
    total_by_order.with_columns(
        col('time').dt.date().alias('date'),
    )
    .groupby('date', maintain_order=True)
    .sum()
    .drop(
        'id',
        'time',
    )
    .with_columns(
        col('total').round(2).alias('total'),
    )
)
export(order_by_date, 'b2c/order_by_date.csv')
