# pyspark

import pyspark.sql.functions as F


def mode_over_window(col_values):
    """

    Use it to get mode of a column in a particular window

    Usage: mode_over_window(F.collect_list(column_name).over(window))
    """

    count = F.transform(col_values, lambda x: F.aggregate(
        F.filter(col_values, lambda y: y == x),
        F.lit(0),
        lambda acc, v: acc + F.lit(1)
    ))

    map_from_array = F.map_from_arrays(col_values, count)

    array_max = F.array_max(F.map_values(map_from_array))

    return F.map_keys(F.map_filter(map_from_array, lambda _, v: v == array_max)).getItem(0)