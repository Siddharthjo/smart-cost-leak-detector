import pandas as pd


def load_csv(file_path: str):
    """
    Reads a CSV file and returns it as a table (DataFrame)
    """
    data = pd.read_csv(file_path)
    return data