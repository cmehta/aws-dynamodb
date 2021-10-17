import pandas as pd


def read_csv(file_name, headerList):
    file = pd.read_csv(file_name)
    file.to_csv(file_name, header=headerList, index=False)
    file_df = pd.read_csv(file_name)
    return file_df


def load_2_dynamo():
    pass


if __name__ == '__main__':
    artist_summary_headerList = ['ArtistSummaryId', 'RoySys', 'Acct_No', 'Acct_Qtr', 'Seq_no', 'Payee_No', 'Owner_name', 'Account_Name', 'Vendor_No', 'Acct_Status', 'Acct_Payee_Status', 'Payee_Status', 'Opening_Bal', 'Prior_Resv', 'Total_Resv', 'Total_Payments', 'Total_Adjustments', 'Dom_Earnings', 'Club_Earnings', '3rd_Party_Earnings', 'Foreign_Earnings', 'Total_Earn', 'Total_Transfers', 'Ending_Bal', 'PCD', 'A_Prior_Resv', 'A_Total_Resv', 'A_Dom_Earnings', 'A_Club_Earnings', 'A_3rd_Party_Earnings', 'A_Foreign_Earnings', 'Payee_Name', 'Address_1', 'Address_2', 'Address_3', 'Address_4', 'Payee_Pct', 'A_Total_Earn', 'Active', 'ASL', 'NonAccrued', 'Total_MiscEarnings', 'A_Total_MiscEarnings', 'IsArchived']
    artist_summary_df = read_csv('artist_summary.csv', artist_summary_headerList)

    artist_details_headerList = ['ArtistDetailId', 'RoySys', 'Acct_No', 'Acct_Qtr', 'Seq_No', 'Payee_No', 'Vendor_No', 'Group_No', 'Source', 'Title', 'Sales Type', 'Price Level', 'Sales Date', 'Selection', 'Config', 'Contract', 'Pr_Code', 'Price', 'Pckg_rate', 'Roy_Rate', 'Part %', 'Eff_rate', 'Tax_rate', 'Net_roy_earn', 'Active', 'ASL', 'DSP Name', 'Units', 'Receipts']
    artist_details_df = read_csv('artist_details.csv', artist_details_headerList)

    artist_summary_df['PK'] = artist_summary_df.RoySys.astype(str) + artist_summary_df.Acct_No.astype(
        str) + artist_summary_df.Acct_Qtr.astype(str) + artist_summary_df.Payee_No.astype(str)
    artist_details_df['PK'] = artist_details_df.RoySys.astype(str) + artist_details_df.Acct_No.astype(
        str) + artist_details_df.Acct_Qtr.astype(str) + artist_details_df.Payee_No.astype(str)

    artist_master_df = pd.merge(artist_summary_df, artist_details_df, how='inner', on='PK')
    print(artist_master_df)