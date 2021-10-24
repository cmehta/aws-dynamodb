import pandas as pd
import boto3
import logging
import json
import decimal

from decimal import Decimal
from botocore.exceptions import ClientError


REGION_NAME = 'us-east-1'
DYNAMO_TABLE = 'poc-strat5'
AWS_S3_BUCKET = 'database-poc-extracts-east1'
logger = logging.getLogger(__name__)


def to_str(val):
    return str(val)


def convert_dtype2_string(df):
    for i in df.columns:
        df[i] = df[i].apply(to_str)


class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, decimal.Decimal):
            return {'__Decimal__': str(obj)}
        # Let the base class default method raise the TypeError
        return json.JSONEncoder.default(self, obj)


def as_Decimal(dct):
    if '__Decimal__' in dct:
        return decimal.Decimal(dct['__Decimal__'])
    return dct


def read_4m_s3(file):
    s3_client = boto3.client('s3')
    response = s3_client.get_object(Bucket=AWS_S3_BUCKET, Key=file)
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    return response, status


def read_csv(file_obj, header_list):
    file_df = pd.read_csv(file_obj, low_memory=False)
    file_df.columns = header_list
    return file_df


def float_to_decimal(num):
    return Decimal(str(num))


def fix_float_2_decimal(df):
    print('fixing types...')
    for i in df.columns:
        print('checking column {}'.format(i))
        datatype = df[i].dtype
        if datatype == 'float64' or datatype == 'int64':
            df[i] = df[i].apply(float_to_decimal)
            print('Types fixed for {}'.format(i))


def add_primarykey(artist_master_df):
    # Adding partition key
    artist_master_df['pk'] = artist_master_df['roysys_x'] + artist_master_df['vendor_no_x'] + artist_master_df['acct_no_x'] +\
                             artist_master_df['acct_qtr_x'] +artist_master_df['dsp_name'] +artist_master_df['title'] + \
                             artist_master_df['source'] + artist_master_df['sales_type']
    # Adding sort key
    artist_master_df['sk'] = artist_master_df['roysys_x'] + artist_master_df['vendor_no_x'] + artist_master_df['acct_no_x']+ \
                             artist_master_df['acct_qtr_x'] + artist_master_df['dsp_name'] + artist_master_df['title'] + \
                             artist_master_df['source'] + artist_master_df['sales_type'] + \
                             artist_master_df['artistdetailid']


def read_data(file, file_header):
    obj, status = read_4m_s3(file)
    if status == 200:
        # logger.info("Successful S3 get_object response for artist summary. Status - %s.", status)
        print("Successful S3 get_object response for {}. Status - {}.".format(file, status))
        df = read_csv(obj.get("Body"), file_header)
        print('Fixing dtypes of {}'.format(file))
        convert_dtype2_string(df)
        print('Fixed dtypes of {}.'.format(file))
    else:
        # logger.info("Unsuccessful S3 get_object response for artist summary. Status - %s.", status)
        print("Unsuccessful S3 get_object response for {}. Status - {}.".format(file, status))
    return df


def batch_load_dynamo(dataframe):
    dynamodb = boto3.resource('dynamodb', region_name=REGION_NAME)
    table = dynamodb.Table(DYNAMO_TABLE)
    # logger.info("Connected to the table - %s.", table.name)
    print("Connected to the table - {}.".format(table.name))
    try:
        with table.batch_writer() as batch:
            for index, row in dataframe.iterrows():
                if index%1000==0:
                    # logger.info("written index - %d.", index)
                    print("written index - {}.".format(index))
                content = {'pk': row['pk'], 'sk': row['sk'], 'artistsummaryid': row['artistsummaryid'],
                           'roysys': row['roysys_x'], 'acct_no': row['acct_no_x'], 'acct_qtr': row['acct_qtr_x'],
                           'seq_no': row['seq_no_x'], 'payee_no': row['payee_no_x'], 'owner_name': row['owner_name'],
                           'account_name': row['account_name'], 'vendor_no': row['vendor_no_x'],
                           'acct_status': row['acct_status'], 'acct_payee_status': row['acct_payee_status'],
                           'payee_status': row['payee_status'], 'opening_bal': row['opening_bal'],
                           'prior_resv': row['prior_resv'], 'total_resv': row['total_resv'],
                           'total_payments': row['total_payments'], 'total_adjustments': row['total_adjustments'],
                           'dom_earnings': row['dom_earnings'], 'club_earnings': row['club_earnings'],
                           '3rd_party_earnings': row['3rd_party_earnings'], 'foreign_earnings': row['foreign_earnings'],
                           'total_earn': row['total_earn'], 'total_transfers': row['total_transfers'],
                           'ending_bal': row['ending_bal'], 'pcd': row['pcd'], 'a_prior_resv': row['a_prior_resv'],
                           'a_total_resv': row['a_total_resv'], 'a_dom_earnings': row['a_dom_earnings'],
                           'a_club_earnings': row['a_club_earnings'],
                           'a_3rd_party_earnings': row['a_3rd_party_earnings'],
                           'a_foreign_earnings': row['a_foreign_earnings'], 'payee_name': row['payee_name'],
                           'address_1': row['address_1'], 'address_2': row['address_2'], 'address_3': row['address_3'],
                           'address_4': row['address_4'], 'payee_pct': row['payee_pct'],
                           'a_total_earn': row['a_total_earn'], 'active': row['active_x'], 'asl': row['asl_x'],
                           'nonaccrued': row['nonaccrued'], 'total_miscearnings': row['total_miscearnings'],
                           'a_total_miscearnings': row['a_total_miscearnings'], 'isarchived': row['isarchived'],
                           'artistdetailid': row['artistdetailid'], 'group_no': row['group_no'],
                           'source': row['source'], 'title': row['title'], 'sales_type': row['sales_type'],
                           'price_level': row['price_level'], 'sales_date': row['sales_date'],
                           'selection': row['selection'], 'config': row['config'], 'contract': row['contract'],
                           'pr_code': row['pr_code'], 'price': row['price'], 'pckg_rate': row['pckg_rate'],
                           'roy_rate': row['roy_rate'], 'part_pct': row['part_pct'], 'eff_rate': row['eff_rate'],
                           'tax_rate': row['tax_rate'], 'net_roy_earn': row['net_roy_earn'],
                           'dsp_name': row['dsp_name'], 'units': row['units'], 'receipts': row['receipts']}

                # Since Object of type Decimal is not JSON serializable, we are using below code to fix this issue.
                # dict_encoded_as_json_string = json.dumps(content, cls=DecimalEncoder)
                # converted_content = json.loads(dict_encoded_as_json_string, object_hook=as_Decimal)
                # print(converted_content)

                batch.put_item(Item=content)
    except ClientError:
        print("Couldn't load data into table {}.".format(table.name))
        # logger.exception("Couldn't load data into table %s.", table.name)
        raise


if __name__ == '__main__':
    artist_summary_headerList = ['artistsummaryid', 'roysys', 'acct_no', 'acct_qtr', 'seq_no', 'payee_no', 'owner_name',
                                 'account_name', 'vendor_no', 'acct_status', 'acct_payee_status', 'payee_status',
                                 'opening_bal', 'prior_resv', 'total_resv', 'total_payments', 'total_adjustments',
                                 'dom_earnings', 'club_earnings', '3rd_party_earnings', 'foreign_earnings',
                                 'total_earn', 'total_transfers', 'ending_bal', 'pcd', 'a_prior_resv', 'a_total_resv',
                                 'a_dom_earnings', 'a_club_earnings', 'a_3rd_party_earnings', 'a_foreign_earnings',
                                 'payee_name', 'address_1', 'address_2', 'address_3', 'address_4', 'payee_pct',
                                 'a_total_earn', 'active', 'asl', 'nonaccrued', 'total_miscearnings',
                                 'a_total_miscearnings', 'isarchived']
    artist_details_headerList = ['artistdetailid', 'roysys', 'acct_no', 'acct_qtr', 'seq_no', 'payee_no', 'vendor_no',
                                 'group_no', 'source', 'title', 'sales_type', 'price_level', 'sales_date', 'selection',
                                 'config', 'contract', 'pr_code', 'price', 'pckg_rate', 'roy_rate', 'part_pct',
                                 'eff_rate', 'tax_rate', 'net_roy_earn', 'active', 'asl', 'dsp_name', 'units',
                                 'receipts']

    artist_summary_df = read_data('artist_summary.csv', artist_summary_headerList)
    artist_details_df = read_data('artist_details.csv', artist_details_headerList)

    artist_summary_df['joinkey'] = artist_summary_df.roysys + artist_summary_df.acct_no + artist_summary_df.acct_qtr\
                              + artist_summary_df.payee_no
    artist_details_df['joinkey'] = artist_details_df.roysys + artist_details_df.acct_no + artist_details_df.acct_qtr \
                              + artist_details_df.payee_no

    artist_master_df = pd.merge(artist_summary_df, artist_details_df, how='inner', on='joinkey')

    add_primarykey(artist_master_df)
    print(artist_master_df.head())
    print(artist_master_df.dtypes)

    batch_load_dynamo(artist_master_df)
