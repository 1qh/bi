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
    date,
    read_csv,
    when,
)

from utils import export

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

store = (
    read_csv(
        'raw/store.csv',
        use_pyarrow=True,
    )
    .drop(
        'store_city',
        'store_state_province',
        'store_postal_code',
        'Neighorhood',
    )
    .rename(
        {
            'store_id': 'id',
            'store_address': 'address',
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
b2c_sales = sales.filter(
    ~col('customer_id').is_in(non_retail['customer_id']),
)


def reid_sales(df: DataFrame, out: str) -> DataFrame:
    new = (
        df.drop(
            'product_id',
            'quantity',
            'price',
            'is_promo',
        )
        .unique(maintain_order=True)
        .sort('time')
    )
    new = (
        new.with_columns(
            Series('_id', range(1, len(new) + 1)),
        )
        .join(
            df,
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
    new = new.select(sorted(new.columns)).rename({'_id': 'id'})
    export(new, out)
    return new


b2b = reid_sales(b2b_sales, 'b2b/sales.csv')
b2c = reid_sales(b2c_sales, 'b2c/sales.csv')


def total_by_order(df: DataFrame, out: str) -> DataFrame:
    new = (
        df.with_columns(
            (col('quantity') * col('price')).alias('total'),
        )
        .groupby(
            'id',
            'time',
            maintain_order=True,
        )
        .sum()
        .select(
            'id',
            'quantity',
            col('total').round(2),
            'time',
        )
        .sort('id')
    )
    export(new, out)
    return new


b2b_total_by_order = total_by_order(b2b, 'b2b/total_by_order.csv')
b2c_total_by_order = total_by_order(b2c, 'b2c/total_by_order.csv')


def order_by_date(df: DataFrame, out: str) -> DataFrame:
    new = (
        df.with_columns(
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
    export(new, out)
    return new


b2b_order_by_date = order_by_date(b2b_total_by_order, 'b2b/order_by_date.csv')
b2c_order_by_date = order_by_date(b2c_total_by_order, 'b2c/order_by_date.csv')


def order_by_month(df: DataFrame, out: str) -> DataFrame:
    new = (
        df.with_columns(
            col('date').dt.month_start().alias('month'),
        )
        .groupby('month', maintain_order=True)
        .sum()
        .drop('date')
        .with_columns(
            col('total').round(2).alias('total'),
        )
    )
    export(new, out)
    return new


b2b_order_by_month = order_by_month(b2b_order_by_date, 'b2b/order_by_month.csv')
b2c_order_by_month = order_by_month(b2c_order_by_date, 'b2c/order_by_month.csv')

rfm = (
    b2c_total_by_order.join(
        b2c,
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


def total_by_product(df: DataFrame, out: str) -> DataFrame:
    new = (
        df.with_columns(
            (col('quantity') * col('price')).alias('total'),
        )
        .groupby(
            'product_id',
            maintain_order=True,
        )
        .sum()
        .select(
            'product_id',
            'quantity',
            col('total').round(2),
        )
        .join(
            product,
            left_on='product_id',
            right_on='id',
        )
        .sort('product_id')
    )
    export(new, out)
    return new


b2b_total_by_product = total_by_product(b2b, 'b2b/total_by_product.csv')
b2c_total_by_product = total_by_product(b2c, 'b2c/total_by_product.csv')


def total_by_customer(df: DataFrame, out: str) -> DataFrame:
    new = (
        df.with_columns(
            (col('quantity') * col('price')).alias('total'),
        )
        .groupby(
            'customer_id',
            maintain_order=True,
        )
        .sum()
        .select(
            'customer_id',
            'quantity',
            col('total').round(2),
        )
        .join(
            customer,
            left_on='customer_id',
            right_on='id',
        )
        .sort('customer_id')
    )
    export(new, out)
    return new


b2b_total_by_customer = total_by_customer(b2b, 'b2b/total_by_customer.csv')
b2c_total_by_customer = total_by_customer(b2c, 'b2c/total_by_customer.csv')
