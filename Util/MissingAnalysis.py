import polars as pl
import pandas as pd
from matplotlib import pyplot as plt
import seaborn as sns

#
def null_analyze(column):
    missing = (data_raw
               .group_by('date_id')
               .agg(pl.col(column).alias(f'missing_{column}_values_counts').is_null().sum())
               .sort("date_id")
               ).collect()
    missing_df = missing.to_pandas()

    # This will draw the number of missing value of this feature over each day
    plt.title("Missing value counts of this feature per day")
    sns.lineplot(data=missing_df, x='date_id', y=f'missing_{column}_values_counts')
    print(missing_df)

    # This will print the missing feature value count per symbol after a date_id
    time_limit = 900
    print((data_raw
           .filter(pl.col('date_id') >= time_limit)
           .group_by('symbol_id')
           .agg(pl.col(column).alias(f'missing_{column}_values_counts').is_null().sum())
           #     .sort(f'missing_{column}_values_counts')
           .sort('symbol_id')
           ).collect().to_pandas())

    # This is check the missing feature value counts at each time id
    missing_time = (data_raw
                    .group_by('time_id')
                    .agg(pl.col(column).alias(f'missing_{column}_values_counts').is_null().sum())
                    .sort('time_id')
                    ).collect()
    print(missing_time.to_pandas().head(50))
    print(missing_time.to_pandas()[50:100])


# Input: the name of two feature columns. Eg: same_missing('feature_01', 'feature_02')
# Return: True if those two features are both missing or neither missing
def same_missing(column1, column2, time_limit=700):
    result = (data_raw
              .filter((pl.col('date_id') >= time_limit) & ((pl.col(column1).is_null()) | (pl.col(column2).is_null())))
              ).collect()

    result_pd = result.to_pandas()
    return (result_pd[column1].notnull().sum() == 0 & result_pd[column2].notnull().sum() == 0)