"""Fabric SQL endpoint write example.

The helper writes rows to a table. Running this module only validates that the
client can be configured without opening a connection.
"""

import pandas as pd


def write_example(db):
    """Write placeholder data after replacing the table name and reviewing the mode."""
    dataframe = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
    result = db.write_dataframe(
        df=dataframe,
        table_name="<target-table>",
        schema="dbo",
        if_exists="append",
        chunksize=10_000,
    )
    print(result)


if __name__ == "__main__":
    print("No database write was run. Edit this file and call write_example with a real table.")
