import pandas as pd
UNNECESSARY_ROWS = [
    'row_id', 
    'nr_validation_vv', 
    '1_battv_avg', 
    '3_airtc_avg',
    '5_ghi_wm2_avg',
    '6_ghi_kwh_tot',   
    '8_gpoa_kwh_tot',
    '9_v_dc_ucy2_07_avg',
    '9_v_dc_ucy2_07_avg',
    '10_i_dc_ucy2_07_avg',
    '12_e_ac_ucy2_07_tot',
    '13_p_dc_ucy2_07_avg',
    '14_e_dc_ucy2_07_tot',
    '15_tmod_ucy2_07_avg'
    ]
RENAME = {
    'time_stamp': 'Datetime',
    '2_ptemp_c_avg': 'Tamb', 
    '4_rh': 'RH',
    '7_gpoa_wm2_avg': 'POA',
    '11_p_ac_ucy2_07_avg': 'Pac'
}

# helper function to process older data files
def _process_old_data(df):
    # Drop the 'Unnamed: 0' column
    df = df.drop(columns=UNNECESSARY_ROWS)
    # Rename the columns to match the new data
    df = df.rename(columns=RENAME)
    return df

# Create dataframe that concatenates all the data from the different sources
def create_data_df():
    df_2022_2023 = pd.read_csv('./data/UCYdemo_2022_2023.csv', parse_dates=[0])
    df_2022_2023 = df_2022_2023.drop(columns=['GHI', 'Pdc', 'Tmod'])
    df_2019 = pd.read_csv('./data/UCYdemo2019.csv', parse_dates=[0])
    df_2019 = _process_old_data(df_2019)
    df_2019['Pac'] = df_2019.apply(lambda x: x['Pac'] / 1000 if x['Datetime'] <= pd.Timestamp('2019-03-15 09:30:00') else x['Pac'], axis=1)
    df_2020 = pd.read_csv('./data/UCYdemo2020.csv', parse_dates=[0])
    df_2020 = _process_old_data(df_2020)
    df_2021 = pd.read_csv('./data/UCYdemo2021.csv', parse_dates=[0])
    df_2021 = _process_old_data(df_2021)

    return pd.concat([df_2019, df_2020, df_2021, df_2022_2023], ignore_index=True)

if __name__ == '__main__':
    df = create_data_df()
    print(df.shape)
    print(df.head())
    print(df.tail())