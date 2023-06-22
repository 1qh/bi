from pprint import pformat

from icecream import ic as p
from polars import DataFrame, read_csv


def custom(o):
    return ('\n' + pformat(o, indent=2)).replace('\n', '\n' + 99 * '\b')


p.configureOutput(
    prefix='',
    argToStringFunction=custom,
)


def overview(df: DataFrame):
    p(df)
    p(df.describe())
    p(df.null_count())
    uniq_count = {}
    uniq_list = {}
    for c in df.columns:
        uniq_count[c] = df[c].n_unique()
        uniq_list[c] = df[c].unique().to_numpy()
    p(DataFrame(uniq_count))
    p(uniq_list)
    p(df.is_duplicated().sum())
    df.glimpse()


def export(df: DataFrame, path: str):
    df.write_csv(path)
    assert read_csv(
        path,
        use_pyarrow=True,
    ).frame_equal(df)
