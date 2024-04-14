import pandas as pd
import pdb

impute_strategy = ['null', 'mode', 'mean', 'zero', 'min', 'max']
categorical_dtypes = ['object', 'category', 'bool']

def read_file(path, table=None):
    """
    Read a file from the specified path based on the given format.

    Parameters:
        path (str): File path.
    Returns:
        DataFrame: DataFrame containing the data read from the file.

    Raises:
        Exception: If there is an error reading the file or if an unsupported file format is provided.
    """
    format = path.split('.')[-1]
    if format == 'csv':
        try:
            df = pd.read_csv(path)
        except:
            raise Exception('Error while trying reading csv file, make sure your file is valid')
 
    elif format == 'xlsx' or format == 'excel':
        try:
            df = pd.read_excel(path)
        except Exception as e:
            raise Exception('Error while trying reading excel file, make sure your file is valid. ', e)
    
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
    
    elif format == 'hdf':
        try:
            df = pd.read_hdf(path)
        except:
            raise Exception('Error while trying reading hdf file, make sure your file is valid')

    else:
        raise Exception("Unsupported File Format")
    return df

def average(df, column):
    """
    Calculate the mean of numeric columns in a DataFrame.

    Parameters:
        df (DataFrame): Input DataFrame.

    Returns:
        Series: Series containing the mean values of numeric columns.
    """
    # Convert columns to numeric
    numeric_data = df[column].apply(pd.to_numeric, errors='coerce')
    
    # Calculate mean of numeric columns
    mean_values = numeric_data.mean()
    
    # Display mean values
    return mean_values

def min(df, column):
    """
    Calculate the minimum value of numeric columns in a DataFrame.

    Parameters:
        df (DataFrame): Input DataFrame.

    Returns:
        Series: Series containing the minimum values of numeric columns.
    """
    # Convert columns to numeric
    numeric_data = df[column].apply(pd.to_numeric, errors='coerce')
    min = numeric_data.min()
    return min

def max(df, column):
    """
    Calculate the maximum value of numeric columns in a DataFrame.

    Parameters:
        df (DataFrame): Input DataFrame.

    Returns:
        Series: Series containing the maximum values of numeric columns.
    """
    numeric_data = df[column].apply(pd.to_numeric, errors='coerce')
    max = numeric_data.max()
    return max

def mode(df, column):
    """
    Calculate the mode of each column in a DataFrame.

    Parameters:
        df (DataFrame): Input DataFrame.

    Returns:
        DataFrame: DataFrame containing the mode of each column.
    """
    return df[column].mode().iloc[0]

def impute(df, column, strategy=1):
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

    if impute_strategy[strategy] == 'mean':
        df[column].fillna(average(df, column), inplace= True)
    elif impute_strategy[strategy] == 'mode':
        df[column].fillna(mode(df, column), inplace= True)
    elif impute_strategy[strategy] == 'min':
        df[column].fillna(min(df, column), inplace= True)
    elif impute_strategy[strategy] == 'max':
        df[column].fillna(max(df, column), inplace= True)
    elif impute_strategy[strategy] == 'zero':
        df[column].fillna(0, inplace= True)    
    else:
        raise Exception("Unsupported impute strategy. Please select one of the following: ", impute_strategy)
    

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
    
    df[column] = df[column].map(mapping)

def one_hot_encode(df, column_name):
    """
    One-hot encode a categorical column.

    Parameters:
        df (DataFrame): Input DataFrame.
        column (str): Name of the categorical column to encode.

    Returns:
        DataFrame: DataFrame with the one-hot encoded columns.
    """

    if column_name in df.columns:
        # Perform one-hot encoding and concatenate with original DataFrame
        df_encoded = pd.get_dummies(df[column_name], prefix=column_name)
        
        # Drop the original categorical column
        df.drop(column_name, axis=1, inplace=True)
        df = pd.concat([df, df_encoded], axis=1)
        pdb.set_trace()
        print("One-hot encoding applied to column ", column_name, " successfully.")
        return df
    else:
        print(f"Column '{column_name}' not found in the DataFrame.")

def label_encode(df, column_name):
    """
    Label encode a categorical column.

    Parameters:
        df (DataFrame): Input DataFrame.
        column (str): Name of the categorical column to encode.

    Returns:
        DataFrame: DataFrame with the label encoded column.
    """
    # df[column] = df[column].astype('category').cat.codes
    if column_name in df.columns:
        # Get unique categories
        categories = df[column_name].unique()
        
        # Create a mapping of categories to integers
        mapping = {category: index for index, category in enumerate(categories)}
        
        # Replace categorical values with their corresponding integer codes
        df[column_name] = df[column_name].map(mapping)
        print(f"Label encoding applied to column '{column_name}' successfully.")
    else:
        print(f"Column '{column_name}' not found in the DataFrame.")

# Function to apply on numerical columns
def numerical_functions(column):
    return {
        'average': column.mean(),
        'min': column.min(),
        'max': column.max(),
        'mode': column.mode().iloc[0]  # Mode
    }

# Function to apply on categorical columns
def categorical_functions(column):
    return {'mode': column.mode().iloc[0]}  # Mode

def main():
    filepath = input('Enter filepath name: ')
    # Handling missing values for continuous variables
    continuous_missing_handling = input('For continuous values, choose a missing value handling technique:\n1. Fill with mode\n2. Fill with mean\n3. Fill with zero\n4. Fill with minimum\n5. Fill with maximum\n0. Drop column\nChoose the technique number: ')

    # Handling missing values for categorical variables
    categorical_missing_handling = input('For categorical values, choose a missing value handling technique:\n1. Fill with mode\n0. Drop column\nChoose the technique number: ')

    drop_columns_list = []
    while True:
        drop_column = input('Do you want to drop any column? y/n:')
        if drop_column == 'y':
            number_of_columns = input('Enter the columns separated by comma:\n')
            user_list = number_of_columns.split(',')
            # convert each item to int type
            for i in range(len(user_list)):
                # convert each item to int type
                drop_columns_list.append(user_list[i])
            break
        elif drop_column == 'n':
            break
        else:
            continue
    
    df = read_file(filepath)
    if len(drop_columns_list) > 0:
        df.drop(drop_columns_list, axis=1, inplace=True)

    # Iterate over columns
    for column_name, column in df.items():
        if pd.api.types.is_numeric_dtype(column):  # Check if numerical
            result = numerical_functions(column)
        else:
            result = categorical_functions(column)
        print(f"For column '{column_name}': {result}")

    if int(continuous_missing_handling) != 0:
        for column_name, column in df.items():
            if pd.api.types.is_numeric_dtype(column):
                impute(df, column_name, int(continuous_missing_handling))

    if int(categorical_missing_handling) == 1:
        for column_name, column in df.items():
            if not pd.api.types.is_numeric_dtype(column):
                impute(df, column_name, int(categorical_missing_handling))

    if int(continuous_missing_handling) == 0:
        for column_name, column in df.items():
            if pd.api.types.is_numeric_dtype(column):
                df = df.dropna(subset=[column_name])

    if int(categorical_missing_handling) == 0:
        for column_name, column in df.items():
           if not pd.api.types.is_numeric_dtype(column):
               df = df.dropna(subset=[column_name])

    for column_name, column in df.items():
        if not pd.api.types.is_numeric_dtype(column):
            categorical_encoding = input('For the following column, choose a categorical data encoding technique:1. Label encoding\n2. One-hot encoding\nChoose the technique number:')
            if int(categorical_encoding) == 1:
                label_encode(df, column_name)
            else:   
                df = one_hot_encode(df, column_name)

    pdb.set_trace()

if __name__ == "__main__":
    main()