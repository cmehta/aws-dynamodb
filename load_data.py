import pandas as pd
import boto3
import awswrangler as wr


def read_4m_s3(aws_s3_bucket, file):
    s3_client = boto3.client('s3')
    response = s3_client.get_object(Bucket=aws_s3_bucket, Key=file)
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    return response, status


def read_csv(file_obj, headerList):
    file_df = pd.read_csv(file_obj)
    file_df.columns = headerList
    return file_df


if __name__ == '__main__':
    DYNAMO_TABLE='poc-artist'
    AWS_S3_BUCKET='database-poc-extracts-east1'
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

    artist_summary_obj, status = read_4m_s3(AWS_S3_BUCKET, 'artist_summary.csv')
    if status == 200:
        print("Successful S3 get_object response for artist summary. Status - {}".format(status))
        artist_summary_df = read_csv(artist_summary_obj.get("Body"), artist_summary_headerList)
    else:
        print("Unsuccessful S3 get_object response for artist summary. Status - {}".format(status))

    artist_details_obj, status = read_4m_s3(AWS_S3_BUCKET, 'artist_details.csv')
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

    wr.dynamodb.put_df(df=artist_master_df, table_name=DYNAMO_TABLE)