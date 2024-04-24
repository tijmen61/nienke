import os
import re
import polars as pl
import tempfile
import zipfile

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


def get_filter_list_from_csv(excel_path, column_name='Author_ID'):
    df = pl.read_excel(excel_path)

    filter_list = df.select(column_name).cast(pl.Int32).to_series().to_list()
    return filter_list


def process_csv(csv_path, columns, filter_list):
    df = pl.read_csv(csv_path, columns=columns)
    print(df.shape)

    df_filtered = df.filter(
        pl.col(columns[0]).is_in(filter_list)
    ).sort(pl.col(columns[0]))

    print(df_filtered.shape)
    return df_filtered


def process_zip(path, filter):
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
    for year_range, categories in rename_rules.items():
        if year in year_range.split(','):
            if category in categories:
                df = df.rename(categories[category])
                break  # Break after finding and applying the correct renaming rules
    return df


def extract_category_and_year(file_name):
    match = re.search(r'OP_DTL_([A-Z]+)_PGYR(\d{4})', file_name)
    if match:
        category = match.group(1)
        year = match.group(2)
        return category, year
    else:
        return None, None


if __name__ == '__main__':
    data_dir = "data"
    results_dir = "results"
    os.makedirs(results_dir, exist_ok=True)

    df_authors = pl.read_csv('AuthorID.csv', separator=';')
    print(df_authors)

    article_columns = [f"ArticleID_{i}" for i in range(1, 27)]  # Adjust the range as necessary
    print(article_columns)

    df_authors_long = df_authors.melt(id_vars=["Author_ID"], value_vars=article_columns,
                                      value_name="Article_ID").filter(
        pl.col("Article_ID").is_not_null())
    print(df_authors_long)

    f_authors_long = df_authors_long.with_columns(
        pl.col("variable").str.extract(r"ArticleID_([0-9]+)", 1).cast(pl.Int32).alias("Article_Num")
    )
    print(f_authors_long)

    # Now group by Author_ID and aggregate the article numbers into lists
    df_authors_grouped = f_authors_long.group_by(["Author_ID"]).agg(
        pl.col("Article_Num").alias("Contributed_Articles")
    )
    print(df_authors_grouped)

    df_authors_final = df_authors_grouped.drop_nulls()
    df_authors_final.write_json("authors.json")
    print(df_authors_final)
    # Verify the result
    # print(df_authors_long)
    #
    # df_authors_grouped = df_authors_long.group_by("Author_ID").agg(
    #     pl.col("Article_ID").alias("Contributed_Articles"))
    # print(df_authors_grouped.head())

    # df_primary_with_articles = df_primary.join(df_authors_grouped, left_on="Profile_ID", right_on="Author_ID",
    #                                            how="left")

    # list_of_ids = get_filter_list_from_csv('author_data.csv')

    # dataframes = []
    #
    # for zip_file in os.listdir(data_dir):
    #     if zip_file.endswith('.ZIP'):
    #         zip_path = os.path.join(data_dir, zip_file)
    #         dataframes.extend(process_zip(zip_path, list_of_ids))
    #
    # combined_df = pl.concat(dataframes, how="diagonal")
    # combined_df.write_csv(os.path.join(results_dir, 'COMBINED.csv'))
    #
    # print(combined_df.shape)
