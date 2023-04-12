# from database import  get_hours, get_timezone, get_status
import csv
import os
from datetime import timedelta, datetime, date, time

import pandas as pd
import pytz

from database import get_store_timezone, get_hrs, get_timestamps, get_all_stores


def get_business_hours(store_id):
    # Load store hours data and timezone data into pandas dataframes
    store_timezone = get_store_timezone(store_id)
    if not store_timezone:
        store_timezone = [{'store_id': store_id, 'timezone': 'America/Chicago'}]
    store_timezone = pd.DataFrame(store_timezone, columns=['store_id', 'timezone'])
    store_timezone['store_id'] = store_timezone['store_id'].astype(str)
    store_hours = get_hrs(store_id)
    if not store_hours:
        # If no hours are specified for this store, assume 24/7
        return {'0': (time.min, time.max), '1': (time.min, time.max), '2': (time.min, time.max),
                '3': (time.min, time.max), '4': (time.min, time.max), '5': (time.min, time.max),
                '6': (time.min, time.max)}
    df = pd.DataFrame(store_hours).sort_values('store_id')
    store_timezone = store_timezone.sort_values('store_id')
    timezone_str = store_timezone['timezone'].iloc[0]
    timezone_ = pytz.timezone(timezone_str)

    # Convert start_time and end_time to UTC
    df['start_time'] = pd.to_datetime(df['start_time_local'], format='%H:%M:%S').dt.time
    df['end_time'] = pd.to_datetime(df['end_time_local'], format='%H:%M:%S').dt.time

    df['start_time_utc'] = df.apply(
        lambda row: timezone_.localize(datetime.combine(datetime.today(), row['start_time'])).astimezone(
            pytz.utc).time() if row['start_time'] != time.min else time.min, axis=1)
    df['end_time_utc'] = df.apply(
        lambda row: timezone_.localize(datetime.combine(datetime.today(), row['end_time'])).astimezone(
            pytz.utc).time() if row['end_time'] != time.max else time.max, axis=1)

    # Create dictionary of business hours
    business_hours = {}
    for day in df['day_of_week']:
        business_hours[str(day)] = (
            df[df['day_of_week'] == day]['start_time_utc'].iloc[0],
            df[df['day_of_week'] == day]['end_time_utc'].iloc[0])

    return business_hours


def get_available_timestamps(store_id):
    business_hours = get_business_hours(store_id)
    timezone = get_store_timezone(store_id)
    if not timezone:
        timezone = 'America/Chicago'
    else:
        timezone = timezone[0]['timezone']
    available_timestamps = []

    # Loop through all available timestamps and return only those that fall within the range of start and end times
    for key, timestamp_str in get_timestamps(store_id)['timestamp_utc'].items():

        for day, hours in business_hours.items():

            day_of_week = timestamp_str.strftime('%w')

            start_time_utc, end_time_utc = hours
            start_datetime_utc = datetime.combine(datetime.today(), start_time_utc) + timedelta(
                days=int(day) - int(day_of_week))
            end_datetime_utc = datetime.combine(datetime.today(), end_time_utc) + timedelta(
                days=int(day) - int(day_of_week))
            start_time_utc = start_datetime_utc.time()
            end_time_utc = end_datetime_utc.time()
            if start_time_utc <= timestamp_str.time() <= end_time_utc:
                available_timestamps.append((timestamp_str, 'active'))
                break

    return available_timestamps


def compute_uptime_downtime(store_id):
    business_hours = get_business_hours(store_id)
    timestamps = get_available_timestamps(store_id)
    current_time = datetime.now().time()
    current_day = datetime.now().weekday()

    # Set initial values for uptime and downtime
    # Initialize uptime and downtime variables
    uptime_last_hour = 0
    downtime_last_hour = 0
    uptime_last_day = 0
    downtime_last_day = 0
    uptime_last_week = 0
    downtime_last_week = 0

    # Calculate one hour ago and one day ago
    one_hour_ago = datetime.now() - timedelta(hours=1)
    one_day_ago = datetime.now() - timedelta(days=1)

    # Iterate over timestamps
    for timestamp in timestamps:
        timestamp_time = timestamp[0].time()
        timestamp_day = timestamp[0].weekday()
        if business_hours[str(timestamp_day)][0] <= timestamp_time <= business_hours[str(timestamp_day)][1]:
            if timestamp[1] == 'active':
                if timestamp[0].hour == current_time.hour:
                    if current_time < business_hours[str(current_day)][0] or current_time > \
                            business_hours[str(current_day)][1]:
                        # Store is in downtime for the current hour
                        downtime_last_hour += 60
                    elif one_hour_ago <= timestamp_time <= current_time:
                        # Store has been active in the last hour
                        uptime_last_hour += (current_time.minute - timestamp_time.minute)
                        downtime_last_day += (datetime.combine(date.today(), current_time) - datetime.combine(
                            date.today(), timestamp_time)).seconds / 3600
                        downtime_last_week += (datetime.combine(date.today(), current_time) - datetime.combine(
                            date.today(), timestamp_time)).seconds / 3600
                elif timestamp[0].hour < current_time.hour:
                    # Store has been active in the last hour
                    uptime_last_hour += 60
                    downtime_last_day += (datetime.combine(date.today(),
                                                           business_hours[str(timestamp_day)][1]) - datetime.combine(
                        date.today(), timestamp_time)).seconds / 3600
                    downtime_last_week += (datetime.combine(date.today(),
                                                            business_hours[str(timestamp_day)][1]) - datetime.combine(
                        date.today(), timestamp_time)).seconds / 3600
            else:
                if timestamp[0].hour == current_time.hour:
                    # Store is in downtime for the current hour
                    downtime_last_hour += 60
                elif timestamp[0].hour < current_time.hour:
                    # Store has been in downtime in the last hour
                    downtime_last_hour += 60
                    downtime_last_day += (datetime.combine(date.today(),
                                                           business_hours[str(timestamp_day)][1]) - datetime.combine(
                        date.today(), timestamp_time)).seconds / 3600
                    downtime_last_week += (datetime.combine(date.today(),
                                                            business_hours[str(timestamp_day)][1]) - datetime.combine(
                        date.today(), timestamp_time)).seconds / 3600
        else:
            if timestamp[0].hour == current_time.hour:
                if (datetime.combine(date.today(), current_time) - datetime.combine(date.today(),
                                                                                    timestamp_time)).seconds // 60 \
                                                                                    <= 60:
                    downtime_last_hour += (datetime.combine(date.today(), current_time) -
                                           datetime.combine(date.today(),
                                           timestamp_time)).seconds // 60
            if timestamp_day == current_day:
                if timestamp[1] == 'active':
                    downtime_last_day += (datetime.combine(date.today(), timestamp_time) - datetime.combine(
                        date.today(), business_hours[str(timestamp_day)][1])).seconds / 3600
                    uptime_last_day += (datetime.combine(date.today(),
                                                         business_hours[str(timestamp_day)][1]) - datetime.combine(
                        date.today(), timestamp_time)).seconds / 3600
                else:
                    downtime_last_day += (datetime.combine(date.today(),
                                                           business_hours[str(timestamp_day)][1]) - datetime.combine(
                        date.today(), business_hours[str(timestamp_day)][0])).seconds / 3600
            if one_day_ago <= timestamp_day <= current_day:
                if timestamp[1] == 'active':
                    downtime_last_week += (datetime.combine(date.today(), timestamp_time) - datetime.combine(
                        date.today(), business_hours[str(timestamp_day)][1])).seconds / 3600
                    uptime_last_week += (datetime.combine(date.today(),
                                                          business_hours[str(timestamp_day)][1]) - datetime.combine(
                        date.today(), timestamp_time)).seconds / 3600
                else:
                    downtime_last_week += (datetime.combine(date.today(),
                                                            business_hours[str(timestamp_day)][1]) - datetime.combine(
                        date.today(), business_hours[str(timestamp_day)][0])).seconds / 3600

    return {'store_id': store_id, 'uptime_last_hour': uptime_last_hour, 'uptime_last_day': uptime_last_day,
            'uptime_last_week': uptime_last_week, 'downtime_last_hour': downtime_last_hour,
            'downtime_last_day': downtime_last_day, 'downtime_last_week': downtime_last_week}


def create_csv(report_id):
    stores = get_all_stores()
    data = []
    for store in stores:
        store_data = compute_uptime_downtime(store)
        store_data['store_id'] = store
        data.append(store_data)

    file_name = f"report_{report_id}.csv"
    fieldnames = ['report_id', 'store_id', 'uptime_last_hour', 'uptime_last_day', 'uptime_last_week', 'downtime_last_hour', 'downtime_last_day',
                  'downtime_last_week']
    try:
        # Write the data to the CSV file
        with open(file_name, mode='w', newline='') as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
            writer.writeheader()
            for store_data in data:
                store_data['report_id'] = report_id
                writer.writerow({key: value for key, value in store_data.items() if key in fieldnames})

    except Exception as e:
        # Handle any exceptions that occur while writing to the CSV file
        print(f"Error writing to CSV file: {e}")
        return "Error"

    # Check if the file was created successfully
    if os.path.exists(file_name):
        return file_name
    else:
        return "Error"

