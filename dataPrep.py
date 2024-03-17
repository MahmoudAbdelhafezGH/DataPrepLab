import pandas as pd
import numpy as np
import sqlite3

impute_strategy = {'mean', 'mode', 'min', 'max', 'zero', 'null'}

def read_file(path, format, table=None):
    """
    Read a file from the specified path based on the given format.

    Parameters:
        path (str): File path.
        format (str): File format (e.g., 'csv', 'xlsx', 'json', 'parquet', etc.).
        table (str or None): Optional. Table name for SQL format.

    Returns:
        DataFrame: DataFrame containing the data read from the file.

    Raises:
        Exception: If there is an error reading the file or if an unsupported file format is provided.
    """
    if format == 'csv':
        try:
            df = pd.read_csv(path)
        except:
            raise Exception('Error while trying reading csv file, make sure your file is valid')
 
    elif format == 'xlsx' or format == 'excel':
        try:
            df = pd.read_excel(path)
        except:
            raise Exception('Error while trying reading excel file, make sure your file is valid')
    
    elif format == 'json':
        try:
            df = pd.read_json(path)
        except:
            raise Exception('Error while trying reading json file, make sure your file is valid')

    elif format == 'parquet':
        try:
            df = pd.read_parquet(path)
        except:
            raise Exception('Error while trying reading Parquet file, make sure your file is valid')
    
    elif format == 'pickle' or format == 'pkl':
        try:
            df = pd.read_pickle(path)
        except:
            raise Exception('Error while trying reading pickle file, make sure your file is valid')
        
    elif format == 'feather':
        try:
            df = pd.read_feather(path)
        except:
            raise Exception('Error while trying reading feather file, make sure your file is valid')

    elif format == 'stata' or format == 'dta':
        try:
            df = pd.read_stata(path)
        except:
            raise Exception('Error while trying reading stata file, make sure your file is valid')
    
    elif format == 'html':
        try:
            df = pd.read_html(path)
        except:
            raise Exception('Error while trying reading html file, make sure your file is valid')

    elif format == 'sql':
        conn = sqlite3.connect(path)
        if table == None:
            raise Exception("Please enter table name in sql data base.")
        else:
            query = "SELECT * FROM " + table
        try:
            df = pd.read_sql(query, conn)
            conn.close()
        except:
            raise Exception('Error while trying reading sql file, make sure your file is valid')
    
    elif format == 'hdf':
        try:
            df = pd.read_hdf(path)
        except:
            raise Exception('Error while trying reading hdf file, make sure your file is valid')

    else:
        raise Exception("Unsupported File Format")
    return df

def average(df):
    """
    Calculate the mean of numeric columns in a DataFrame.

    Parameters:
        df (DataFrame): Input DataFrame.

    Returns:
        Series: Series containing the mean values of numeric columns.
    """
    # Convert columns to numeric
    numeric_data = df.apply(pd.to_numeric, errors='coerce')
    
    # Calculate mean of numeric columns
    mean_values = numeric_data.mean()
    
    # Display mean values
    return mean_values

def min(df):
    """
    Calculate the minimum value of numeric columns in a DataFrame.

    Parameters:
        df (DataFrame): Input DataFrame.

    Returns:
        Series: Series containing the minimum values of numeric columns.
    """
    # Convert columns to numeric
    numeric_data = df.apply(pd.to_numeric, errors='coerce')
    min = numeric_data.min()
    return min

def max(df):
    """
    Calculate the maximum value of numeric columns in a DataFrame.

    Parameters:
        df (DataFrame): Input DataFrame.

    Returns:
        Series: Series containing the maximum values of numeric columns.
    """
    numeric_data = df.apply(pd.to_numeric, errors='coerce')
    max = numeric_data.max()
    return max

def mode(df):
    """
    Calculate the mode of each column in a DataFrame.

    Parameters:
        df (DataFrame): Input DataFrame.

    Returns:
        DataFrame: DataFrame containing the mode of each column.
    """
    return df.mode()

def impute(df, strategy='zero'):
    """
    Impute missing values in a DataFrame based on the specified strategy.

    Parameters:
        df (DataFrame): Input DataFrame.
        strategy (str): Imputation strategy. Options are 'mean', 'mode', 'min', 'max', or 'zero'.
                        Default is 'zero'.

    Returns:
        DataFrame: DataFrame with missing values filled based on the chosen strategy.
    
    Raises:
        Exception: If an unsupported imputation strategy is provided.
    """
    if strategy == 'mean':
        filled_df = df.fillna(average(df))
    elif strategy == 'mode':
        filled_df = df.fillna(mode(df))
    elif strategy == 'min':
        filled_df = df.fillna(min(df))
    elif strategy == 'max':
        filled_df = df.fillna(max(df))
    elif strategy == 'zero':
        filled_df = df.fillna(0)    
    else:
        raise Exception("Unsupported impute strategy. Please select one of the following: ", impute_strategy)
    return filled_df

def remove_missing_values(df):
    """
    Remove rows containing missing values (NaN) from a DataFrame.

    Parameters:
        df (DataFrame): Input DataFrame.

    Returns:
        DataFrame: DataFrame with rows containing missing values removed.
    """
    return df.dropna()

def ordinal_encode(df, column, mapping=None):
    """
    Ordinal encode a categorical column based on a provided mapping.
    If mapping is not provided, generate mapping from unique categories.

    Parameters:
        df (DataFrame): Input DataFrame.
        column (str): Name of the categorical column to encode.
        mapping (dict or None): Optional. Dictionary mapping categories to numerical values.
                                If None, generate mapping from unique categories.

    Returns:
        DataFrame: DataFrame with the encoded column.
        dict: Mapping used for encoding.
    """
    if mapping is None:
        unique_categories = df[column].unique()
        mapping = {category: i for i, category in enumerate(unique_categories)}
    else:
        # Ensure mapping is a dictionary
        if not isinstance(mapping, dict):
            raise ValueError("Mapping must be a dictionary.")
    
    encoded_column = df[column].map(mapping)
    return encoded_column, mapping

def one_hot_encode(df, column):
    """
    One-hot encode a categorical column.

    Parameters:
        df (DataFrame): Input DataFrame.
        column (str): Name of the categorical column to encode.

    Returns:
        DataFrame: DataFrame with the one-hot encoded columns.
    """
    encoded_columns = pd.get_dummies(df[column], prefix=column)
    return encoded_columns

def label_encode(df, column):
    """
    Label encode a categorical column.

    Parameters:
        df (DataFrame): Input DataFrame.
        column (str): Name of the categorical column to encode.

    Returns:
        DataFrame: DataFrame with the label encoded column.
    """
    encoded_column = df[column].astype('category').cat.codes
    return encoded_column

def main():
    df = read_file('cars.parquet', 'parquet', 'my_table')
    print(average(df))
    print(min(df))
    print(max(df))
    print(mode(df))
    print(remove_missing_values(mode(df).loc[:,['model', 'mpg']]))
    # Ordinal Encoding with automatic mapping generation
    encoded_column = one_hot_encode(df, 'model')
    
    print("Encoded column:")
    print(encoded_column)
if __name__ == "__main__":
    main()