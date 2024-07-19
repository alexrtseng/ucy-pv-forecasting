import pandas as pd
import math
import logging
import requests
import time

log = logging.getLogger(__name__)

START = pd.Timestamp('2023-11-02 12:30:00')
END = pd.Timestamp('2024-7-04 09:30:00')

XWEATHER_CLIENT_ID = 'SK97xhuiT4ZtBULDLHvhi'
XWEATHER_CLIENT_SECRET = '4ZSPcFgnNS4gJOLSDpa6XMxxfMEQ4EpOYirMt46U'
XWEATHER_BASE_URL = "https://api.aerisapi.com/"  # Different from one in data_collection (for batch feature)

# Make batch (requests) param for API call (signifigantly reduces number of calls to API)
def get_weather_data_batch(start: pd.Timestamp, end: pd.Timestamp, longitude, latitude, resolution, min_resolution: bool) -> pd.DataFrame | bool:    
    num_required_calls = math.ceil(((end - start).seconds / 3600) + (end - start).days * 24) if min_resolution else (end - start).days + 1
    assert num_required_calls <= 31, "Number of requests must be under 31"
    assert num_required_calls > 0, "Number of requests must be greater than 0"
    log.debug(f'Num of calls in batch: {num_required_calls}')
    batch_param_base_url = f'/conditions/{longitude},{latitude}'  # CHANGE TO FORECAST FOR FUTURE PREDICTIONS
    requests_param = ''
    for i in range(num_required_calls):
        timedelta_from_args = start + pd.Timedelta(**({'hours': i} if min_resolution else {'days': i}))
        from_param = f'{timedelta_from_args}'
        timedelta_to_args = end if i == num_required_calls - 1 else start + pd.Timedelta(**({'hours': i + 1} if min_resolution else {'days': i + 1}))
        to_param = f'{timedelta_to_args}'

        requests_param += f'{batch_param_base_url}%3Ffrom={from_param}%26to={to_param}'
        if not i == num_required_calls - 1:
            requests_param += ','

    # Global (first 4) & batch params (last) for API call (not in batch requests)
    filter_type = 'min' if min_resolution else 'hr'
    params = {
        "format": "json",
        "filter": f'{resolution}{filter_type}',
        "client_id": XWEATHER_CLIENT_ID,
        "client_secret": XWEATHER_CLIENT_SECRET,
        "requests": requests_param
    }

    # Make API call
    try: 
        response = requests.get(
            f"{XWEATHER_BASE_URL}/batch",
            params=params,
        )
        if response.status_code != 200:
            log.error(
                f"Failed to get weather data for Long: {longitude}, Lat: {latitude}, Status: {response.status_code}"
            )
            return False
        response_data = response.json()
        if response_data['success'] != True:
            log.error(
                f"Error in response package for Long: {longitude}, Lat: {latitude}, Recieved error: {response['error']}"
            )
            return False
        
    except Exception as e:
        log.error(f'Failed to get weather data for Long: {longitude}, Lat: {latitude}, Error: {e}')
        return False
    
    return response_data

# Return dataframe with weather features and datetime column given a time range and resolution
def get_weather_features(start, end) -> pd.DataFrame:
    # Get weather data from API
    
    response_data = get_weather_data_batch(start=start, end=end, longitude=33.416462, latitude=35.146155, resolution=15, min_resolution=True)

    # Create dataframe from API batched responses
    datetime = []
    tamb_series = []
    rh_series = []
    precip_info = []
    for individual_response in response_data['response']['responses']:
        if individual_response['success'] == True:
            for period in individual_response['response'][0]['periods']:
                rh_series.append(period['humidity'])
                precip_info.append(period['precipMM'])
                datetime.append(period['dateTimeISO'])    # ALTERNATIVE IS ValidTime?
        else:
            log.error(
                f"Error in individual response for, Recieved error: {individual_response['error']}"
            )

    df = pd.DataFrame({
        'datetime': datetime,
        'humidity': rh_series,
        'precipMM': precip_info
    })
    df['datetime'] = (pd
                        .to_datetime(df['datetime'], utc=True)
                    )

    return df

def get_all_weather():

    # Get weather data from API for the entire time range
    df = get_weather_features(pd.Timestamp('2023-07-02 23:00:00'), pd.Timestamp('2023-07-03 15:00:00'))
    #df = get_weather_features(START + pd.Timedelta(days=0), START + pd.Timedelta(days=0, hours=23, minutes=30))
    #df = pd.concat([df, get_weather_features(START + pd.Timedelta(days=244), END + pd.Timedelta(hours=3))], ignore_index=True)
    # for i in range(1, 80):
    #     start = START + pd.Timedelta(days=i)
    #     end = start + pd.Timedelta(hours=23, minutes=45)
    #     df = pd.concat([df, get_weather_features(start, end)], ignore_index=True)
    #     print(f'Finished {end} days')
    #     time.sleep(5)
        

    return df


if __name__ == '__main__':
    df = get_all_weather()
    df['datetime'] = df['datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')
    df.to_csv('./data/UCY_SM_Weather_play.csv', index=False)
                                                   