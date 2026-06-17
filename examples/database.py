"""Database example: write a pandas DataFrame to a Fabric SQL endpoint."""

import pandas as pd

from examples._setup import build_clients


def write_example(db):
    df = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
    result = db.write_dataframe(
        df=df,
        table_name="my_table",
        schema="dbo",
        if_exists="append",
        chunksize=10000,
    )
    print(result)


if __name__ == "__main__":
    clients = build_clients()
    write_example(clients["db"])
