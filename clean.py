from polars import (
    DataFrame,
    Date,
    Datetime,
    Float64,
    Int32,
    Series,
    col,
    concat,
    concat_str,
    count,
    read_csv,
    when,
)


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


sales = (
    concat(
        [
            read_csv(
                f'raw/sales/202{i}.csv',
                use_pyarrow=True,
            ).with_columns(
                concat_str(
                    [
                        col('transaction_date'),
                        when(col('transaction_time').str.count_match(':') == 1)
                        .then(col('transaction_time') + ':00')
                        .otherwise(col('transaction_time')),
                    ]
                )
                .str.strptime(
                    Datetime,
                    format='%m/%d/%Y %H:%M:%S',
                )
                .alias('time'),
                col('unit_price').str.replace(',', '.').cast(Float64),
            )
            for i in range(3)
        ]
    )
    .with_columns(
        col('promo_item_yn').map_dict(
            {
                'N': False,
                'Y': True,
            }
        ),
    )
    .drop_nulls()
    .unique(maintain_order=True)
    .drop(
        'transaction_date',
        'transaction_time',
    )
    .rename(
        {
            'promo_item_yn': 'is_promo',
            'unit_price': 'price',
            'transaction_id': 'id',
            'quantity_sold': 'quantity',
        }
    )
    .sort('time')
)
non_retail = (
    sales.groupby('customer_id', maintain_order=True)
    .count()
    .filter(col('count') > 1000)
)
export(non_retail, 'findings/non_retail.csv')

b2b_sales = sales.filter(
    col('customer_id').is_in(non_retail['customer_id']),
)
b2b = (
    b2b_sales.drop(
        'product_id',
        'quantity',
        'price',
        'is_promo',
    )
    .unique(maintain_order=True)
    .sort('time')
)
b2b = (
    b2b.with_columns(
        Series('_id', range(1, len(b2b) + 1)),
    )
    .join(
        b2b_sales,
        on=[
            'time',
            'id',
            'store_id',
            'staff_id',
            'customer_id',
        ],
    )
    .drop('id')
)
b2b = b2b.select(sorted(b2b.columns)).rename({'_id': 'id'})
export(b2b, 'b2b/sales.csv')

b2b_total_by_order = (
    b2b.with_columns(
        (col('quantity') * col('price')).alias('total'),
    )
    .groupby('id', maintain_order=True)
    .sum()
    .select(
        col('id'),
        col('total').round(2),
    )
    .sort('id')
)
export(b2b_total_by_order, 'b2b/total_by_order.csv')

b2b_total_by_customer = (
    b2b.with_columns(
        (col('quantity') * col('price')).alias('total'),
    )
    .groupby('customer_id', maintain_order=True)
    .sum()
    .select(
        col('customer_id'),
        col('total').round(2),
    )
    .sort('customer_id')
)
export(b2b_total_by_customer, 'b2b/total_by_customer.csv')

b2c_sales = sales.filter(
    ~col('customer_id').is_in(non_retail['customer_id']),
)
b2c = (
    b2c_sales.drop(
        'product_id',
        'quantity',
        'price',
        'is_promo',
    )
    .unique(maintain_order=True)
    .sort('time')
)
b2c = (
    b2c.with_columns(
        Series('_id', range(1, len(b2c) + 1)),
    )
    .join(
        b2c_sales,
        on=[
            'time',
            'id',
            'store_id',
            'staff_id',
            'customer_id',
        ],
    )
    .drop('id')
)
b2c = b2c.select(sorted(b2c.columns)).rename({'_id': 'id'})
export(b2c, 'b2c/sales.csv')

b2c_total_by_order = (
    b2c.with_columns(
        (col('quantity') * col('price')).alias('total'),
    )
    .groupby('id', maintain_order=True)
    .sum()
    .select(
        col('id'),
        col('total').round(2),
    )
    .sort('id')
)
export(b2c_total_by_order, 'b2c/total_by_order.csv')

b2c_total_by_customer = (
    b2c.with_columns(
        (col('quantity') * col('price')).alias('total'),
    )
    .groupby('customer_id', maintain_order=True)
    .sum()
    .select(
        col('customer_id'),
        col('total').round(2),
    )
    .sort('customer_id')
)
export(b2c_total_by_customer, 'b2c/total_by_customer.csv')

customer = (
    read_csv(
        'raw/customer.csv',
        use_pyarrow=True,
    )
    .with_columns(
        col(
            'birthdate',
            'customer_since',
        ).str.strptime(
            Date,
            format='%m/%d/%Y',
        ),
        col('gender').str.replace(
            'Not Specified',
            'na',
        ),
    )
    .drop_nulls()
    .unique(maintain_order=True)
    .rename(
        {
            'customer_id': 'id',
            'home_store': 'store',
            'customer_since': 'since',
        }
    )
    .with_columns(
        age=2022 - col('birthdate').dt.year().cast(Int32),
    )
    .drop(
        'birthdate',
        'customer_first-name',
        'customer_email',
        'loyalty_card_number',
    )
    .sort('id')
)
export(customer, 'data/customer.csv')

employee = (
    read_csv(
        'raw/employee.csv',
        use_pyarrow=True,
    )
    .with_columns(
        col('start_date').str.strptime(
            Date,
            format='%m/%d/%Y',
        )
    )
    .drop(
        'first_name',
        'last_name',
        'end_date',
    )
    .drop_nulls()
    .unique(maintain_order=True)
    .rename(
        {
            'staff_id': 'id',
            'start_date': 'onboard',
        }
    )
    .sort('id')
)
export(employee, 'data/employee.csv')

store = (
    read_csv(
        'raw/store.csv',
        use_pyarrow=True,
    )
    .drop(
        'store_address',
        'store_city',
        'store_state_province',
        'store_postal_code',
        'Neighorhood',
    )
    .rename(
        {
            'store_id': 'id',
            'store_type': 'type',
            'store_square_feet': 'square_feet',
            'store_longitude': 'longitude',
            'store_latitude': 'latitude',
        }
    )
    .unique(maintain_order=True)
    .sort('id')
)
export(store, 'data/store.csv')

product = (
    read_csv(
        'raw/product.csv',
        use_pyarrow=True,
    )
    .with_columns(
        col(
            'tax_exempt_yn',
            'promo_yn',
            'new_product_yn',
        ).map_dict(
            {
                'N': False,
                'Y': True,
            }
        )
    )
    .drop_nulls()
    .unique(maintain_order=True)
    .drop('product_description')
    .rename(
        {
            'product_id': 'id',
            'product_group': 'group',
            'product_category': 'category',
            'product_type': 'type',
            'product': 'name',
            'unit_of_measure': 'unit',
            'current_cost': 'cost',
            'current_wholesale_price': 'wholesale',
            'current_retail_price': 'retail',
            'tax_exempt_yn': 'is_tax_exempt',
            'promo_yn': 'is_promo',
            'new_product_yn': 'is_new',
        }
    )
    .sort('id')
)
export(product, 'data/product.csv')
