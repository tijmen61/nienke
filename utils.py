"""
This script processes zipped CSV files containing physician payment data,
extracts relevant information, and combines it with author data to generate a final CSV file.
"""

import os
import re
import polars as pl  # Polars is a DataFrame library optimized for performance
import tempfile
import zipfile

# Dictionary mapping year ranges to their respective column names for different categories
columns_dict = {
    '2013,2014,2015': {
        "GNRL": [
            "Physician_Profile_ID",
            "Physician_First_Name",
            "Physician_Middle_Name",
            "Physician_Last_Name",
            "Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name",
            "Total_Amount_of_Payment_USDollars",
            "Form_of_Payment_or_Transfer_of_Value",
            "Nature_of_Payment_or_Transfer_of_Value",
            "Product_Indicator",
            "Name_of_Associated_Covered_Drug_or_Biological1",
            "Name_of_Associated_Covered_Device_or_Medical_Supply1",
            "Program_Year"
        ],
        "RSRCH": [
            "Principal_Investigator_1_Profile_ID",
            "Principal_Investigator_1_First_Name",
            "Principal_Investigator_1_Middle_Name",
            "Principal_Investigator_1_Last_Name",
            "Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name",
            "Product_Indicator",
            "Name_of_Associated_Covered_Drug_or_Biological1",
            "Name_of_Associated_Covered_Device_or_Medical_Supply1",
            "Total_Amount_of_Payment_USDollars",
            "Form_of_Payment_or_Transfer_of_Value",
            "Name_of_Study",
            "Program_Year"
        ],
        "OWNRSHP": [
            'Physician_Profile_ID',
            'Physician_First_Name',
            'Physician_Middle_Name',
            'Physician_Last_Name',
            'Program_Year',
            'Total_Amount_Invested_USDollars',
            'Value_of_Interest',
            'Terms_of_Interest',
            'Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name'
        ]
    },
    '2016,2017,2018,2019,2020,2021,2022': {
        "GNRL": [
            'Covered_Recipient_Profile_ID',
            'Covered_Recipient_NPI',
            'Covered_Recipient_First_Name',
            'Covered_Recipient_Middle_Name',
            'Covered_Recipient_Last_Name',
            'Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name',
            'Total_Amount_of_Payment_USDollars',
            'Form_of_Payment_or_Transfer_of_Value',
            'Nature_of_Payment_or_Transfer_of_Value',
            'Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_1',
            'Product_Category_or_Therapeutic_Area_1',
            'Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_1',
            'Program_Year'
        ],
        "RSRCH": [
            'Principal_Investigator_1_Profile_ID',
            'Principal_Investigator_1_NPI',
            'Principal_Investigator_1_First_Name',
            'Principal_Investigator_1_Middle_Name',
            'Principal_Investigator_1_Last_Name',
            'Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name',
            'Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_1',
            'Product_Category_or_Therapeutic_Area_1',
            'Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_1',
            'Total_Amount_of_Payment_USDollars',
            'Form_of_Payment_or_Transfer_of_Value',
            'Name_of_Study',
            'Program_Year'
        ],
        "OWNRSHP": [
            'Physician_Profile_ID',
            'Physician_NPI',
            'Physician_First_Name',
            'Physician_Middle_Name',
            'Physician_Last_Name',
            'Program_Year',
            'Total_Amount_Invested_USDollars',
            'Value_of_Interest',
            'Terms_of_Interest',
            'Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name'
        ]
    },
}

# Dictionary for renaming columns based on the year and category
rename_rules = {
    '2013,2014,2015': {
        'GNRL': {
            'Physician_Profile_ID': 'Profile_ID',
            'Physician_First_Name': 'First_Name',
            'Physician_Middle_Name': 'Middle_Name',
            'Physician_Last_Name': 'Last_Name'
        },
        'RSRCH': {
            'Principal_Investigator_1_Profile_ID': 'Profile_ID',
            'Principal_Investigator_1_First_Name': 'First_Name',
            'Principal_Investigator_1_Middle_Name': 'Middle_Name',
            'Principal_Investigator_1_Last_Name': 'Last_Name'
        },
        'OWNRSHP': {
            'Physician_Profile_ID': 'Profile_ID',
            'Physician_First_Name': 'First_Name',
            'Physician_Middle_Name': 'Middle_Name',
            'Physician_Last_Name': 'Last_Name'
        }
    },
    '2016,2017,2018,2019,2020,2021,2022': {
        'GNRL': {
            'Covered_Recipient_Profile_ID': 'Profile_ID',
            'Covered_Recipient_NPI': 'NPI',
            'Covered_Recipient_First_Name': 'First_Name',
            'Covered_Recipient_Middle_Name': 'Middle_Name',
            'Covered_Recipient_Last_Name': 'Last_Name'
        },
        'RSRCH': {
            'Principal_Investigator_1_Profile_ID': 'Profile_ID',
            'Principal_Investigator_1_NPI': 'NPI',
            'Principal_Investigator_1_First_Name': 'First_Name',
            'Principal_Investigator_1_Middle_Name': 'Middle_Name',
            'Principal_Investigator_1_Last_Name': 'Last_Name'
        },
        'OWNRSHP': {
            'Physician_Profile_ID': 'Profile_ID',
            'Physician_NPI': 'NPI',
            'Physician_First_Name': 'First_Name',
            'Physician_Middle_Name': 'Middle_Name',
            'Physician_Last_Name': 'Last_Name'
        }
    },
}


def process_csv(csv_path, columns, filter_list):
    """
    Process a CSV file to filter and sort its data.

    :param csv_path: Path to the CSV file.
    :param columns: List of columns to read from the CSV file.
    :param filter_list: List of filter values for the first column.
    :return: Filtered DataFrame.
    """
    df = pl.read_csv(csv_path, columns=columns)
    print(df.shape)

    df_filtered = df.filter(
        pl.col(columns[0]).is_in(filter_list)
    ).sort(pl.col(columns[0]))

    print(df_filtered.shape)
    return df_filtered


def process_zip(path, filter):
    """
    Process a ZIP file containing CSV files, extract relevant data and apply filters.

    :param path: Path to the ZIP file.
    :param filter: List of filter values for the first column in CSV files.
    :return: List of processed DataFrames.
    """
    with tempfile.TemporaryDirectory() as temp_dir:
        with zipfile.ZipFile(path, 'r') as zip_ref:
            zip_ref.extractall(temp_dir)

        res = []
        for csv_file in os.listdir(temp_dir):
            if csv_file.endswith('.csv'):
                csv_path = os.path.join(temp_dir, csv_file)
                category, year = extract_category_and_year(csv_file)

                columns_to_use = []
                if category and year:
                    print(category, year)

                    for year_range, categories in columns_dict.items():
                        if year in year_range.split(','):
                            columns_to_use = categories[category]
                            break

                if columns_to_use:
                    processed_df = process_csv(csv_path, columns_to_use, filter)
                    processed_df = processed_df.with_columns(pl.lit(category).alias("Category"))
                    processed_df = rename_cols(processed_df, category, year)
                    res.append(processed_df)
    return res


def rename_cols(df, category, year):
    """
    Rename columns in the DataFrame based on the provided category and year.

    :param df: DataFrame to rename columns.
    :param category: Data category (GNRL, RSRCH, OWNRSHP).
    :param year: Year of the data.
    :return: DataFrame with renamed columns.
    """
    for year_range, categories in rename_rules.items():
        if year in year_range.split(','):
            if category in categories:
                df = df.rename(categories[category])
                break  # Break after finding and applying the correct renaming rules
    return df


def extract_category_and_year(file_name):
    """
    Extract the category and year from the file name.

    :param file_name: File name string.
    :return: Tuple of (category, year) if matched, otherwise (None, None).
    """
    match = re.search(r'OP_DTL_([A-Z]+)_PGYR(\d{4})', file_name)
    if match:
        category = match.group(1)
        year = match.group(2)
        return category, year
    else:
        return None, None


def extract_author_articles_and_ids(csv_path):
    """
    Extract author articles and IDs from a CSV file.

    :param csv_path: Path to the author CSV file.
    :return: DataFrame of authors and their articles, and a filter list of author IDs.
    """
    df_authors = pl.read_csv(csv_path, separator=';')
    article_columns = [col for col in df_authors.columns if col.startswith('ArticleID_')]
    df_authors_long = df_authors.melt(id_vars=["Author_ID"], value_vars=article_columns,
                                      value_name="Article_ID").filter(
        pl.col("Article_ID").is_not_null())

    f_authors_long = df_authors_long.with_columns(
        pl.col("variable").str.extract(r"ArticleID_([0-9]+)", 1).cast(pl.Int32).alias("Article_Num")
    )

    df_authors_grouped = f_authors_long.group_by(["Author_ID"]).agg(
        pl.col("Article_Num").alias("Contributed_Articles")
    )

    # Extract the filter list of Author IDs
    filter_list = df_authors.select("Author_ID").cast(pl.Int32).to_series().to_list()

    return df_authors_grouped, filter_list


def extract_date_from_filename(filename):
    """
    Extract date from the filename.

    :param filename: Filename string.
    :return: Extracted date string in YYYYMMDD format if found, otherwise None.
    """
    match = re.search(r'\d{8}', filename)
    if match:
        return match.group(0)
    return None


if __name__ == '__main__':
    data_dir = "data"  # Directory containing the data ZIP files
    results_dir = "results"  # Directory to save the results
    os.makedirs(results_dir, exist_ok=True)

    author_id_file = 'authors/AuthorID_20240628.csv'
    date_str = extract_date_from_filename(author_id_file)

    # Extract author data and list of IDs
    df_authors, list_of_ids = extract_author_articles_and_ids(author_id_file)

    dataframes = []

    # Process each ZIP file in the data directory
    for zip_file in os.listdir(data_dir):
        if zip_file.endswith('.ZIP'):
            zip_path = os.path.join(data_dir, zip_file)
            dataframes.extend(process_zip(zip_path, list_of_ids))

    # Combine all processed DataFrames
    combined_df = pl.concat(dataframes, how="diagonal")

    # Join with author data
    df_primary_with_articles = combined_df.join(df_authors, left_on="Profile_ID", right_on="Author_ID", how="left")

    # Convert list of contributed articles to string
    df_primary_with_articles_string = df_primary_with_articles.with_columns(
        pl.col("Contributed_Articles").map_elements(lambda x: ', '.join(map(str, x))).alias("Contributed_Articles")
    )

    final_filename = f'FINAL_{date_str}.csv'
    df_primary_with_articles_string.write_csv(os.path.join(results_dir, final_filename))

    print("FINAL SIZE: ", combined_df.shape)
    print(f"File saved as {final_filename}")
