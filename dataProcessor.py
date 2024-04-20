import pandas as pd

class DataProcessor:
    impute_strategy = ['null', 'mode', 'mean', 'zero', 'min', 'max']
    
    def __init__(self):
        pass

    @staticmethod
    def read_file(path):
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

        else:
            raise Exception("Unsupported File Format")
        return df

    @staticmethod
    def write_file(path, format, df):
        if format == 'csv':
            try:
                df.to_csv(path)
            except:
                raise Exception('Error while trying writing csv file, make sure your file is valid')

        elif format == 'xlsx' or format == 'excel':
            try:
                df.to_excel(path)
            except Exception as e:
                raise Exception('Error while trying writing excel file, make sure your file is valid. ', e)

        elif format == 'json':
            try:
                df.to_json(path)
            except:
                raise Exception('Error while trying writing json file, make sure your file is valid')

        elif format == 'parquet':
            try:
                df.to_parquet(path)
            except:
                raise Exception('Error while trying writing Parquet file, make sure your file is valid')

        elif format == 'pickle' or format == 'pkl':
            try:
                df.to_pickle(path)
            except:
                raise Exception('Error while trying writing pickle file, make sure your file is valid')

        elif format == 'feather':
            try:
                df.to_feather(path)
            except:
                raise Exception('Error while trying writing feather file, make sure your file is valid')

        elif format == 'stata' or format == 'dta':
            try:
                df.to_stata(path)
            except:
                raise Exception('Error while trying writing stata file, make sure your file is valid')

        elif format == 'html':
            try:
                df.to_html(path)
            except:
                raise Exception('Error while trying writing html file, make sure your file is valid')

        else:
            raise Exception("Unsupported File Format")

    @staticmethod
    def average(df, column):
        """
        Calculate the mean of numeric columns in a DataFrame.

        Parameters:
            df (DataFrame): Input DataFrame.
            column (str): Name of the numeric column.

        Returns:
            float: Mean value of the specified column.
        """
        numeric_data = pd.to_numeric(df[column], errors='coerce')
        return numeric_data.mean()

    @staticmethod
    def min(df, column):
        """
        Calculate the minimum value of numeric columns in a DataFrame.

        Parameters:
            df (DataFrame): Input DataFrame.
            column (str): Name of the numeric column.

        Returns:
            float: Minimum value of the specified column.
        """
        numeric_data = pd.to_numeric(df[column], errors='coerce')
        return numeric_data.min()

    @staticmethod
    def max(df, column):
        """
        Calculate the maximum value of numeric columns in a DataFrame.

        Parameters:
            df (DataFrame): Input DataFrame.
            column (str): Name of the numeric column.

        Returns:
            float: Maximum value of the specified column.
        """
        numeric_data = pd.to_numeric(df[column], errors='coerce')
        return numeric_data.max()

    @staticmethod
    def mode(df, column):
        """
        Calculate the mode of each column in a DataFrame.

        Parameters:
            df (DataFrame): Input DataFrame.
            column (str): Name of the column.

        Returns:
            object: Mode value of the specified column.
        """
        return df[column].mode().iloc[0]

    @staticmethod
    def impute(df, column, strategy=1):
        """
        Impute missing values in a DataFrame based on the specified strategy.

        Parameters:
            df (DataFrame): Input DataFrame.
            column (str): Name of the column.
            strategy (int): Imputation strategy index. Default is 1 (mode).

        Returns:
            DataFrame: DataFrame with missing values filled based on the chosen strategy.

        Raises:
            Exception: If an unsupported imputation strategy is provided.
        """
        if DataProcessor.impute_strategy[strategy] == 'mean':
            df[column].fillna(DataProcessor.average(df, column), inplace=True)
        elif DataProcessor.impute_strategy[strategy] == 'mode':
            df[column].fillna(DataProcessor.mode(df, column), inplace=True)
        elif DataProcessor.impute_strategy[strategy] == 'min':
            df[column].fillna(DataProcessor.min(df, column), inplace=True)
        elif DataProcessor.impute_strategy[strategy] == 'max':
            df[column].fillna(DataProcessor.max(df, column), inplace=True)
        elif DataProcessor.impute_strategy[strategy] == 'zero':
            df[column].fillna(0, inplace=True)
        else:
            raise Exception("Unsupported impute strategy. Please select one of the following: ", DataProcessor.impute_strategy)
        
    @staticmethod
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
            if not isinstance(mapping, dict):
                raise ValueError("Mapping must be a dictionary.")

        df[column] = df[column].map(mapping)

    @staticmethod
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
            df_encoded = pd.get_dummies(df[column_name], prefix=column_name, dtype=int)
            df.drop(column_name, axis=1, inplace=True)
            df = pd.concat([df, df_encoded], axis=1)
            print("One-hot encoding applied to column ", column_name, " successfully.")
            return df
        else:
            print(f"Column '{column_name}' not found in the DataFrame.")

    @staticmethod
    def label_encode(df, column_name):
        """
        Label encode a categorical column.

        Parameters:
            df (DataFrame): Input DataFrame.
            column (str): Name of the categorical column to encode.

        Returns:
            DataFrame: DataFrame with the label encoded column.
        """
        if column_name in df.columns:
            categories = df[column_name].unique()
            mapping = {category: index for index, category in enumerate(categories)}
            df[column_name] = df[column_name].map(mapping)
            print(f"Label encoding applied to column '{column_name}' successfully.")
        else:
            print(f"Column '{column_name}' not found in the DataFrame.")

    @staticmethod
    def numerical_functions(column):
        return {
            'average': column.mean(),
            'min': column.min(),
            'max': column.max(),
            'mode': column.mode().iloc[0]  # Mode
        }
        
    @staticmethod
    def categorical_functions(column):
        return {'mode': column.mode().iloc[0]}  # Mode
