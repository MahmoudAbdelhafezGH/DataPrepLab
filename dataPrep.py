import pandas as pd
from dataProcessor import DataProcessor

impute_strategy = ['null', 'mode', 'mean', 'zero', 'min', 'max']
categorical_dtypes = ['object', 'category', 'bool']
file_formats = {
    '1': 'csv',
    '2': 'xlsx',
    '3': 'json',
    '4': 'parquet',
    '5': 'pickle',
    '6': 'feather',
    '7': 'stata',
    '8': 'html',
}

def main():
    filepath = input('Enter filepath name: ')
    # Handling missing values for continuous variables
    continuous_missing_handling = input('For continuous values, choose a missing value handling technique:\n1. Fill with mode\n2. Fill with mean\n3. Fill with zero\n4. Fill with minimum\n5. Fill with maximum\n0. Drop row\nChoose the technique number: ')

    # Handling missing values for categorical variables
    categorical_missing_handling = input('For categorical values, choose a missing value handling technique:\n1. Fill with mode\n0. Drop row\nChoose the technique number: ')

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
    
    df = DataProcessor.read_file(filepath)
    if len(drop_columns_list) > 0:
        df.drop(drop_columns_list, axis=1, inplace=True)
        print(f"Dataframe after dropping the columns: {df}")

    # Iterate over columns
    for column_name, column in df.items():
        if pd.api.types.is_numeric_dtype(column):  # Check if numerical
            result = DataProcessor.numerical_functions(column)
        else:
            result = DataProcessor.categorical_functions(column)
        print(f"For column '{column_name}': {result}")

    if int(continuous_missing_handling) != 0:
        for column_name, column in df.items():
            if pd.api.types.is_numeric_dtype(column):
                DataProcessor.impute(df, column_name, int(continuous_missing_handling))

    if int(categorical_missing_handling) == 1:
        for column_name, column in df.items():
            if not pd.api.types.is_numeric_dtype(column):
                DataProcessor.impute(df, column_name, int(categorical_missing_handling))

    if int(continuous_missing_handling) == 0:
        for column_name, column in df.items():
            if pd.api.types.is_numeric_dtype(column):
                df = df.dropna(subset=[column_name])

    if int(categorical_missing_handling) == 0:
        for column_name, column in df.items():
           if not pd.api.types.is_numeric_dtype(column):
               df = df.dropna(subset=[column_name])

    print("Dataframe after handling the missing values: ")
    print(df)
    for column_name, column in df.items():
        if not pd.api.types.is_numeric_dtype(column):
            categorical_encoding = input('For the following column, choose a categorical data encoding technique:1. Label encoding\n2. One-hot encoding\nChoose the technique number:')
            if int(categorical_encoding) == 1:
                DataProcessor.label_encode(df, column_name)
            else:   
                df = DataProcessor.one_hot_encode(df, column_name)

    print("Dataframe after encoding categorical data: ")
    print(df)

    # Display available file formats
    print("Choose the file format to save the DataFrame:")
    print("1. CSV")
    print("2. Excel")
    print("3. JSON")
    print("4. Parquet")
    print("5. Pickle")
    print("6. Feather")
    print("7. Stata")
    print("8. HTML")

    # Prompt user to enter a number corresponding to the desired format
    choice = input("Enter the number corresponding to the desired file format: ")

    if choice in file_formats:
        file_format = file_formats[choice]
        file_name = input("Enter the file name (without extension): ")
        file_path = f"{file_name}.{file_format}"
        DataProcessor.write_file(file_path, file_format, df)
        print(f"DataFrame saved successfully to '{file_path}'.")
    else:
        print("Invalid choice. Please enter a number corresponding to the desired file format.")

if __name__ == "__main__":
    main()