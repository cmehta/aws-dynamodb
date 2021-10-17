import pandas as pd
import boto3

from decimal import Decimal


REGION_NAME = 'us-east-1'
DYNAMO_TABLE = 'poc-artist'
AWS_S3_BUCKET = 'database-poc-extracts-east1'


def read_4m_s3(file):
    s3_client = boto3.client('s3')
    response = s3_client.get_object(Bucket=AWS_S3_BUCKET, Key=file)
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    return response, status


def read_csv(file_obj, header_list):
    file_df = pd.read_csv(file_obj)
    file_df.columns = header_list
    return file_df


def float_to_decimal(num):
    return Decimal(str(num))


def fix_float_2_decimal(df):
    for i in df.columns:
        datatype = df[i].dtype
        if datatype == 'float64':
            df[i] = df[i].apply(float_to_decimal)
    return df


def batch_load_dynamo(dataframe):
    dynamodb = boto3.resource('dynamodb', region_name=REGION_NAME)
    table = dynamodb.Table(DYNAMO_TABLE)
    with table.batch_writer() as batch:
        for index, row in dataframe.iterrows():
            content = {'ArtistSummaryId': row['ArtistSummaryId'], 'RoySys': row['RoySys_x'],
                       'Acct_No': row['Acct_No_x'], 'Acct_Qtr_x': row['Acct_Qtr_x'], 'Seq_no': row['Seq_no'],
                       'Payee_No': row['Payee_No_x'], 'Owner_name': row['Owner_name'],
                       'Account_Name': row['Account_Name'], 'Vendor_No': row['Vendor_No_x'],
                       'Acct_Status': row['Acct_Status'], 'Acct_Payee_Status': row['Acct_Payee_Status'],
                       'Payee_Status': row['Payee_Status'], 'Opening_Bal': row['Opening_Bal'],
                       'Prior_Resv': row['Prior_Resv'], 'Total_Resv': row['Total_Resv'],
                       'Total_Payments': row['Total_Payments'], 'Total_Adjustments': row['Total_Adjustments'],
                       'Dom_Earnings': row['Dom_Earnings'], 'Club_Earnings': row['Club_Earnings'],
                       '3rd_Party_Earnings': row['3rd_Party_Earnings'], 'Foreign_Earnings': row['Foreign_Earnings'],
                       'Total_Earn': row['Total_Earn'], 'Total_Transfers': row['Total_Transfers'],
                       'Ending_Bal': row['Ending_Bal'], 'PCD': row['PCD'], 'A_Prior_Resv': row['A_Prior_Resv'],
                       'A_Total_Resv': row['A_Total_Resv'], 'A_Dom_Earnings': row['A_Dom_Earnings'],
                       'A_Club_Earnings': row['A_Club_Earnings'], 'A_3rd_Party_Earnings': row['A_3rd_Party_Earnings'],
                       'A_Foreign_Earnings': row['A_Foreign_Earnings'], 'Payee_Name': row['Payee_Name'],
                       'Address_1': row['Address_1'], 'Address_2': row['Address_2'], 'Address_3': row['Address_3'],
                       'Address_4': row['Address_4'], 'Payee_Pct': row['Payee_Pct'],
                       'A_Total_Earn': row['A_Total_Earn'], 'Active_x': row['Active_x'], 'ASL_x': row['ASL_x'],
                       'NonAccrued': row['NonAccrued'], 'Total_MiscEarnings': row['Total_MiscEarnings'],
                       'A_Total_MiscEarnings': row['A_Total_MiscEarnings'], 'IsArchived': row['IsArchived'],
                       'PK': row['PK'], 'ArtistDetailId': row['ArtistDetailId'], 'Seq_No': row['Seq_No'],
                       'Group_No': row['Group_No'], 'Source': row['Source'], 'Title': row['Title'],
                       'Sales Type': row['Sales Type'], 'Price Level': row['Price Level'],
                       'Sales Date': row['Sales Date'], 'Selection': row['Selection'], 'Config': row['Config'],
                       'Contract': row['Contract'], 'Pr_Code': row['Pr_Code'], 'Price': row['Price'],
                       'Pckg_rate': row['Pckg_rate'], 'Roy_Rate': row['Roy_Rate'], 'Part %': row['Part %'],
                       'Eff_rate': row['Eff_rate'], 'Tax_rate': row['Tax_rate'], 'Net_roy_earn': row['Net_roy_earn'],
                       'DSP Name': row['DSP Name'], 'Units': row['Units'], 'Receipts': row['Receipts']}
            batch.put_item(Item=content)


if __name__ == '__main__':
    artist_summary_headerList = ['ArtistSummaryId', 'RoySys', 'Acct_No', 'Acct_Qtr', 'Seq_no', 'Payee_No', 'Owner_name',
                                 'Account_Name', 'Vendor_No', 'Acct_Status', 'Acct_Payee_Status', 'Payee_Status',
                                 'Opening_Bal', 'Prior_Resv', 'Total_Resv', 'Total_Payments', 'Total_Adjustments',
                                 'Dom_Earnings', 'Club_Earnings', '3rd_Party_Earnings', 'Foreign_Earnings',
                                 'Total_Earn', 'Total_Transfers', 'Ending_Bal', 'PCD', 'A_Prior_Resv', 'A_Total_Resv',
                                 'A_Dom_Earnings', 'A_Club_Earnings', 'A_3rd_Party_Earnings', 'A_Foreign_Earnings',
                                 'Payee_Name', 'Address_1', 'Address_2', 'Address_3', 'Address_4', 'Payee_Pct',
                                 'A_Total_Earn', 'Active', 'ASL', 'NonAccrued', 'Total_MiscEarnings',
                                 'A_Total_MiscEarnings', 'IsArchived']
    artist_details_headerList = ['ArtistDetailId', 'RoySys', 'Acct_No', 'Acct_Qtr', 'Seq_No', 'Payee_No', 'Vendor_No',
                                 'Group_No', 'Source', 'Title', 'Sales Type', 'Price Level', 'Sales Date', 'Selection',
                                 'Config', 'Contract', 'Pr_Code', 'Price', 'Pckg_rate', 'Roy_Rate', 'Part %',
                                 'Eff_rate', 'Tax_rate', 'Net_roy_earn', 'Active', 'ASL', 'DSP Name', 'Units',
                                 'Receipts']

    artist_summary_obj, status = read_4m_s3('artist_summary.csv')
    if status == 200:
        print("Successful S3 get_object response for artist summary. Status - {}".format(status))
        artist_summary_df = read_csv(artist_summary_obj.get("Body"), artist_summary_headerList)
    else:
        print("Unsuccessful S3 get_object response for artist summary. Status - {}".format(status))

    artist_details_obj, status = read_4m_s3('artist_details.csv')
    if status == 200:
        print("Successful S3 get_object response for artist details. Status - {}".format(status))
        artist_details_df = read_csv(artist_details_obj.get("Body"), artist_details_headerList)
    else:
        print("Unsuccessful S3 get_object response for artist details. Status - {}".format(status))

    artist_summary_df['PK'] = artist_summary_df.RoySys.astype(str) + artist_summary_df.Acct_No.astype(
        str) + artist_summary_df.Acct_Qtr.astype(str) + artist_summary_df.Payee_No.astype(str)
    artist_details_df['PK'] = artist_details_df.RoySys.astype(str) + artist_details_df.Acct_No.astype(
        str) + artist_details_df.Acct_Qtr.astype(str) + artist_details_df.Payee_No.astype(str)

    artist_master_df = pd.merge(artist_summary_df, artist_details_df, how='inner', on='PK')
    print(artist_master_df.head())

    artist_master_df = fix_float_2_decimal(artist_master_df)

    batch_load_dynamo(artist_master_df)
