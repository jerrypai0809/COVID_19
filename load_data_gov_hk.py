import requests
import pandas as pd
import re
import xml.etree.ElementTree as ET
import urllib


def load_data_gov_to_csv(data: str, save: bool):

    url = 'https://api.data.gov.hk/v2/filter'

    if data == 'daily_hk_cases':
        params = {
           'q': '{"resource":"http://www.chp.gov.hk/files/misc/enhanced_sur_pneumonia_wuhan_eng.csv",'
                '"section":1,"format":"json"}'
        }

    elif data == 'daily_hk_building':
        params = {
            'q': '{"resource":"http://www.chp.gov.hk/files/misc/building_list_eng.csv","section":1,"format":"json"}'
        }

    elif data == 'daily_cn_cases':
        params = {
            'q': '{"resource":"http://www.chp.gov.hk/files/misc/areas_in_mainland_china_have_reported_cases_eng.csv",'
                 '"section":1,"format":"json"}'
        }

    elif data == 'daily_outside_cn_cases':
        params = {
            'q': '{"resource":"http://www.chp.gov.hk/files/misc/'
                 'countries_areas_outside_mainland_china_have_reported_cases_eng.csv",'
                 '"section":1,"format":"json"}'
        }

    elif data == 'daily_infected_pub_transportation':
        params = {
            'q': '{"resource":"http://www.chp.gov.hk/files/misc/flights_trains_list_eng.csv",'
                 '"section":1,"format":"json"}'
        }

    response = requests.get(url, params=params)
    response_json = response.json()
    # print(response_json)
    # print(type(response_json))

    result_pd = pd.DataFrame(response_json)

    # print(result_pd)

    if save:
        result_pd.to_csv('data\\' + data + '.csv', index=False)

    return result_pd


def strip_case_no(org_pd: pd.DataFrame):
    # pass
    # org_pd['Related probable/confirmed cases'] = org_pd['Related probable/confirmed cases'].str.extract('(\d+)')
    # print(org_pd['Related probable/confirmed cases'])
    for i, row in org_pd.iterrows():

        temp = org_pd.at[i, 'Related probable/confirmed cases']

        org_pd.at[i, 'Related probable/confirmed cases'] = re.sub('[^0-9,]', "", temp)

    # print(org_pd)
    return org_pd

def splitDataFrameList(df, target_column,separator):
    """ df = dataframe to split,
    target_column = the column containing the values to split
    separator = the symbol used to perform the split
    returns: a dataframe with each entry for the target column separated, with each element moved into a new row.
    The values in the other columns are duplicated across the newly divided rows.
    """

    def splitListToRows(row, row_accumulator, target_column, separator):
        split_row = row[target_column].split(separator)
        for s in split_row:
            new_row = row.to_dict()
            new_row[target_column] = s
            row_accumulator.append(new_row)
    new_rows = []
    df.apply(splitListToRows,axis=1, args= (new_rows, target_column, separator))
    new_df = pd.DataFrame(new_rows)
    return new_df


def insert_lat_long(org_pd: pd.DataFrame):

    df_comp = org_pd.copy(deep=True)
    # df_comp['address_long'] = df_comp[]

    df_comp['long_address'] = df_comp['Building name'].str.replace(' (non-residential)', '', case = False) + ', ' + \
                              df_comp['District']

    print(df_comp)

    for i, row in df_comp.iterrows():

        temp = df_comp.at[i, 'long_address']

        df_comp.at[i, 'LatLong'] = return_lat_long(temp)

    return df_comp


def return_lat_long(long_address: str):

    print('Querying ' + long_address)

    query = urllib.parse.quote(long_address)

    # print(query)

    url_str = 'https://www.als.ogcio.gov.hk//lookup?n=1&q=' + query

    addr = requests.get(url_str)

    xml_ = addr.text
    print(xml_)
    parser = ET.XMLParser()
    tree = ET.ElementTree(ET.fromstring(xml_, parser=parser))

    root = tree.getroot()

    suggestedAddress = root.find('SuggestedAddress')
    address = suggestedAddress.find('Address')
    premisesAddress = address.find('PremisesAddress')
    geospatialInformation = premisesAddress.find('GeospatialInformation')
    latitude = geospatialInformation.find('Latitude').text
    longitude = geospatialInformation.find('Longitude').text

    #  print(latitude)
    # print(longitude)

    return str(latitude) + ',' + str(longitude)


def district_data_cleaning(org_pd: pd.DataFrame):

    df_comp = org_pd.copy(deep=True)
    df_comp = df_comp.replace({"&Western": "& Western", "and Western": "& Western", "islands": "island"}, regex=True)
    df_comp['District'] = df_comp['District'].str.strip()

    return df_comp


def split_dept_arri(org_pd: pd.DataFrame):

    df_comp = org_pd.copy(deep=True)

    foo = lambda x: pd.Series([i for i in (x.split('→'))])
    df = df_comp['Departure & arrival'].apply(foo)
    df.columns = ['Destination ' + str(col) for col in df.columns]
    # df_comp['Departure'], df_comp['Arrival'] = df_comp['Departure & arrival'].str.split("→", expand=True)
    # df_comp['Departure'] = df_comp['Departure'].str.strip()
    # df_comp['Arrival'] = df_comp['Arrival'].str.strip()

    result = pd.concat([df_comp, df], axis=1)
    # print(result)
    return result


def main():

    # load daily hk cases
    print('start to load daily hk cases')
    load_data_gov_to_csv('daily_hk_cases', True)
    print('finished loading daily hk cases')

    # load daily mainland cases
    print('start to load daily mainland cases')
    load_data_gov_to_csv('daily_cn_cases', True)
    print('finished loading daily mainland cases')

    # load daily international cases
    print('start to load daily international cases')
    load_data_gov_to_csv('daily_outside_cn_cases', True)
    print('finished loading daily international cases')

    # load daily transportation
    print('start to load daily infected public transportation')
    pub_trans_pd = load_data_gov_to_csv('daily_infected_pub_transportation', False)
    print('finished loading daily infected public transportation')

    # pd.set_option("display.max_columns", None, "display.max_rows", None)
    # print(pub_trans_pd)

    # print('Split departure and arrival')
    pub_trans_pd = split_dept_arri(pub_trans_pd)

    # strip case no
    print('start to strip case no')
    pub_trans_pd = strip_case_no(pub_trans_pd)
    print('finished stripping case no')

    # stack case no.
    print('start to stack case no')
    pub_trans_pd = splitDataFrameList(pub_trans_pd, 'Related probable/confirmed cases', ',')
    print('finished stacking case no')

    print('start to save final infected public transportation data to csv')
    pub_trans_pd.to_csv('data\\daily_infected_pub_trans.csv', index=False)

    # load daily hk building
    print('start to load daily hk building')
    building_pd = load_data_gov_to_csv('daily_hk_building', False)
    print('finished loading daily hk building')

    # district data cleaning
    building_pd = district_data_cleaning(building_pd)

    # load lat and long
    print('start to load latlong')
    building_pd = insert_lat_long(building_pd)
    print('finished loading latlong')

    # strip case no.
    print('start to strip case no')
    building_pd = strip_case_no(building_pd)  # need to change later!!!
    print('finished stripping case no')

    # stack case no.
    print('start to stack case no')
    building_pd = splitDataFrameList(building_pd, 'Related probable/confirmed cases', ',')
    print('finished stacking case no')

    print(building_pd)

    print('start to save final building data to csv')
    building_pd.to_csv('data\\daily_hk_building.csv', index=False)


if __name__ == '__main__':
    main()

