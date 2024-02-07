"""
When you run the script manually (with Data Analysis and save file 'selected_currency_data.csv'),
you can simply execute: 'python main.py --manual'
And if you want to run it automatically, you can use: 'python main.py --auto'
For help run 'python main.py -h, --help'
"""
import argparse
import os
import pandas as pd
import requests
import schedule
import time

from datetime import datetime, timedelta
from prettytable import PrettyTable


ITERATION_LIMIT = 50
ALL_CURRENCY_FILE = 'all_currency_data.csv'
SELECTED_CURRENCY_FILE = 'selected_currency_data.csv'


def fetch_currency_data(currency_code, start_date, end_date):
    url = f'https://api.nbp.pl/api/exchangerates/rates/A/{currency_code}/{start_date}/{end_date}/?format=json'

    response = requests.get(url)
    try:
        response.raise_for_status()
        if response.status_code == 404:
            raise Exception('\nData not found for the specified time range.')
        elif response.status_code == 400 and 'Przekroczony limit' in response.text:
            raise Exception('\nRequest limit exceeded. Please reduce the time range.')
    except requests.exceptions.HTTPError as http_err:
        print(f'\nHTTP error occurred while fetching data for {currency_code}. Status code: {response.status_code}')
        raise http_err
    except Exception as err:
        print(f'\nAn error occurred while fetching data for {currency_code}: {err}')
        raise err

    data = response.json()
    rates = data.get('rates', [])
    if not rates:
        raise Exception(f'\nNo exchange rate data found for {currency_code}')

    return {entry['effectiveDate']: entry['mid'] for entry in rates}


def save_to_csv(data_frame, filename):
    if filename == SELECTED_CURRENCY_FILE:
        try:
            if os.path.isfile(filename):
                data_frame.to_csv(filename, index=False)
                print(f'\nData has been successfully updated to {filename}')
            else:
                data_frame.to_csv(filename, index=False)
                print(f'\nData has been successfully saved to {filename}')
        except Exception as e:
            print(f'\nError while saving data to {filename}: {e}')

    elif filename == ALL_CURRENCY_FILE:
        try:
            if os.path.isfile(filename):
                existing_data = pd.read_csv(filename)
                latest_date_existing = pd.to_datetime(existing_data['Date']).max()

                new_entries = data_frame[pd.to_datetime(data_frame['Date']) > latest_date_existing]

                updated_data = pd.concat([existing_data, new_entries]).drop_duplicates(subset=['Date'])

                updated_data.to_csv(filename, index=False)
                print(f'\nData has been successfully updated to {filename}')
            else:
                data_frame.to_csv(filename, index=False)
                print(f'\nData has been successfully saved to {filename}')
        except Exception as e:
            print(f'\nError while saving data to {filename}: {e}')


def analyze_currency_pair(data_frame, selected_currency_pairs):
    table = PrettyTable()
    table.field_names = ['Currency Pair', 'Average Rate', 'Median Rate', 'Minimum Rate', 'Maximum Rate']

    for currency_pair in selected_currency_pairs:
        selected_columns = ['Date', currency_pair]

        if currency_pair not in data_frame.columns:
            raise ValueError(f'\nThe currency pair {currency_pair} is not present in the DataFrame.')

        selected_data = data_frame[selected_columns].copy()

        if selected_data.empty:
            raise ValueError(f'\nNo data available for the currency pair {currency_pair}.')

        selected_data['Date'] = pd.to_datetime(selected_data['Date'])
        statistics = selected_data.describe().transpose()

        table.add_row(
            [
                currency_pair,
                f"{statistics['mean'].iloc[1]:.4f}",
                f"{statistics['50%'].iloc[1]:.4f}",
                f"{statistics['min'].iloc[1]:.4f}",
                f"{statistics['max'].iloc[1]:.4f}",
            ]
        )

    print(table)


def get_default_date_range():
    start_date = (datetime.now() - timedelta(days=90)).strftime('%Y-%m-%d')
    end_date = datetime.now().strftime('%Y-%m-%d')
    return start_date, end_date


def process_currency_data():
    start_date, end_date = get_default_date_range()

    eur_rates = fetch_currency_data('EUR', start_date, end_date)
    usd_rates = fetch_currency_data('USD', start_date, end_date)
    chf_rates = fetch_currency_data('CHF', start_date, end_date)

    eur_usd_rates = {date: round(eur_rates[date] / usd_rates[date], 4) for date in eur_rates.keys()}
    chf_usd_rates = {date: round(chf_rates[date] / usd_rates[date], 4) for date in chf_rates.keys()}

    df = pd.DataFrame(
        {
            'Date': list(eur_rates.keys()),
            'EUR/PLN': list(eur_rates.values()),
            'USD/PLN': list(usd_rates.values()),
            'CHF/PLN': list(chf_rates.values()),
            'EUR/USD': list(eur_usd_rates.values()),
            'CHF/USD': list(chf_usd_rates.values()),
        }
    )

    return df


def main_auto():
    df = process_currency_data()
    save_to_csv(df, ALL_CURRENCY_FILE)


def main_manual():
    df = process_currency_data()
    save_to_csv(df, ALL_CURRENCY_FILE)

    user_input = input('\nEnter the currency pairs you want to analyze (comma-separated, e.g., EUR/PLN,USD/PLN): ')
    if not user_input:
        raise ValueError('\nNo currency pairs provided.')

    selected_currency_pairs = [pair.replace(' ', '').upper() for pair in user_input.split(',')]
    if not selected_currency_pairs:
        raise ValueError('\nNo valid currency pairs provided.')

    columns_to_drop = [col for col in df.columns if col not in ['Date'] + selected_currency_pairs]
    filtered_df = df.drop(columns=columns_to_drop)

    save_to_csv(filtered_df, SELECTED_CURRENCY_FILE)

    analyze_currency_pair(filtered_df, selected_currency_pairs)


def main():
    parser = argparse.ArgumentParser(description='Currency Data Analysis')
    parser.add_argument('--auto', action='store_true', help='Run in automatic mode, without Data Analysis')
    parser.add_argument('--manual', action='store_true', help='Run in manual mode with Data Analysis and filtering')

    args = parser.parse_args()

    if args.auto:
        """
        Uncomment this code for quick tests
        """
        # schedule.every(5).seconds.do(main_auto)
        # i = 0
        # while i < ITERATION_LIMIT:
        #     schedule.run_pending()
        #     i += 1
        #     time.sleep(1)
        # print(f"Auto mode completed {i} iterations.")

        schedule.every().day.at('12:00').do(main_auto)
        while True:
            schedule.run_pending()
            time.sleep(1)
    elif args.manual:
        main_manual()
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
