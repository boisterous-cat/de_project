import pandas as pd
import psycopg2
import os
import shutil
from datetime import datetime


def define_paths(isArchive=False):
    # Get the directory of the current script
    current_directory = os.path.dirname(os.path.abspath(__file__))

    # Navigate up to the project root directory
    project_root_directory = os.path.abspath(os.path.join(current_directory, '..'))

    # directory=""
    if isArchive:
        directory = os.path.join(project_root_directory, 'archive')
    else:
        # Specify the data directory
        directory = os.path.join(project_root_directory, 'data')

    if not os.path.exists(directory):
        os.makedirs(directory)

    return directory


def read_xlsx(file_prefix='terminals'):
    data_directory = define_paths()

    all_df = []  # List to hold valid DataFrames

    for filename in os.listdir(data_directory):
        if filename.startswith(file_prefix):
            excel_file = os.path.join(data_directory, filename)

            if not os.path.exists(excel_file):
                print(f"Warning: The file '{excel_file}' was not found before processing.")
                continue  # Skip to the next file if it does not exist
            # df = pd.read_excel(excel_file, sheet_name=file_prefix)
            try:
                df = pd.read_excel(excel_file)
                date_str = os.path.splitext(filename)[0].split('_')[-1]

                if file_prefix.startswith("terminals"):
                    df['update_dt'] = datetime.strptime(date_str, '%d%m%Y').strftime('%Y-%m-%d %H:%M:%S')

                if file_prefix.startswith("passport_blacklist"):
                    df = df.rename(columns={"date": "entry_dt", "passport": "passport_num"})

                backup_filename = f"{filename}.backup"

                archive_directory = define_paths(isArchive=True)

                shutil.move(excel_file, os.path.join(archive_directory, backup_filename))
                print(f"File moved to archive as {backup_filename}.")

                # Add the DataFrame to the list
                all_df.append(df)
                # os.remove(terminals_file)

            except pd.errors.EmptyDataError:
                print(f"Error: The file '{filename}' is empty or not readable.")
            except Exception as e:
                print(e)
                # print(f"An error occurred while processing '{filename}': {e}")

    # Combine all DataFrames into one
    if all_df:
        combined_df = pd.concat(all_df, ignore_index=True)
        return combined_df
    else:
        print("No valid data was processed.")
        return pd.DataFrame()


def read_csv(file_prefix='transaction'):
    data_directory = define_paths()

    all_df = []  # List to hold valid DataFrames

    for filename in os.listdir(data_directory):
        if filename.startswith(file_prefix):
            csv_file = os.path.join(data_directory, filename)

            if not os.path.exists(csv_file):
                print(f"Warning: The file '{csv_file}' was not found before processing.")
                continue  # Skip to the next file if it does not exist

            try:
                df = pd.read_csv(csv_file, sep=';')
                # date_str = os.path.splitext(filename)[0].split('_')[-1]
                # df['update_dt'] = datetime.strptime(date_str, '%d%m%Y').strftime('%Y-%m-%d %H:%M:%S')

                if file_prefix.startswith("transaction"):
                    df = df.rename(
                        columns={'transaction_id': 'trans_id', 'transaction_date': 'trans_date', 'amount': 'amt'})
                    df['amt'] = df['amt'].replace(',', '.', regex=True)
                    df = df.astype({"amt": float})

                backup_filename = f"{filename}.backup"

                archive_directory = define_paths(isArchive=True)

                shutil.move(csv_file, os.path.join(archive_directory, backup_filename))
                print(f"File moved to archive as {backup_filename}.")

                # Add the DataFrame to the list
                all_df.append(df)
                # os.remove(terminals_file)

            except pd.errors.EmptyDataError:
                print(f"Error: The file '{filename}' is empty or not readable.")
            except Exception as e:
                print(e)

    # Combine all DataFrames into one
    if all_df:
        combined_df = pd.concat(all_df, ignore_index=True)
        return combined_df
    else:
        print("No valid data was processed.")
        return pd.DataFrame()

