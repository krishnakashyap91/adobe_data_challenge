import re
from datetime import datetime
from pytz import timezone
from io import StringIO
import pandas as pd
import boto3

# Mapping for search engines and its query paramertes and whitespace characters
DOMAIN_PARAMETER_MAPPING = {"www.google.com" : ["q", "+"], 
                            "www.bing.com" : ["q", "+"], 
                            "search.yahoo.com" : ["p", "+"]}
CURRENT_DATE = datetime.now(timezone('America/Denver')).strftime('%Y-%m-%d')
FILENAME = f"{CURRENT_DATE}_SearchKeywordPerformance.tab"
BUCKET = "data-adobe-analytics"

def read_data(client, bucket, key):
    """
    Method to read file from S3 bucket. Returns data frame
    """
    s3_clientobj = client.get_object(Bucket=bucket, Key=key)
    # Read data from S3 folder
    bytes_data = s3_clientobj["Body"].read()
    string_data=str(bytes_data,"utf-8")
    data = StringIO(string_data)
    df=pd.read_csv(data, sep="\t")
    return df


def get_domain(url):
    """
    Extract domain from referrer link. Returns domain in lower case.
    """
    domain = ''
    groups = re.search(r"https?:\/\/([A-Za-z_0-9.-]+).*", url)
    if groups:
        domain = groups.group(1)
    if domain in DOMAIN_PARAMETER_MAPPING.keys():
        return str.lower(domain)
    else:
        return ""
        
def get_revenue(product_list):
    """
    Extract revenue from product list. Returns revenue.
    """
    if isinstance(product_list, str):
        product_list_array = product_list.split(";")
        revenue = 0 if product_list_array[3] == ""  else float(product_list_array[3])
        return revenue
    else:
        return 0
        
def get_search_keyword(url):
    """
    Extract search keyword from referrer link. Returns keyword in lower case.
    """
    keyword = ''
    domain = get_domain(url)
    if domain in DOMAIN_PARAMETER_MAPPING.keys():
        groups = re.search(f"{DOMAIN_PARAMETER_MAPPING[domain][0]}=([A-Za-z_0-9+.-]+)", url)
        if groups:
            keyword = groups.group(1).replace(DOMAIN_PARAMETER_MAPPING[domain][1],
                                              " ")
        return str.lower(keyword)
    else:
        return ""


def lambda_handler(event, context):
    # Create S3 client
    s3_client = boto3.client("s3")
    # Read data
    df=read_data(s3_client, BUCKET, "analytics/data.sql")
    # Generate report
    # Extract domain from referrer
    df["Search Engine Domain"] = df["referrer"].apply(get_domain)
    # Extract revenue from product_list
    df["Revenue"] = df["product_list"].apply(get_revenue)
    # Extract search keyword from url
    df["Search Keyword"] = df["referrer"].apply(get_search_keyword)
    report_df = df[["Search Engine Domain", "Search Keyword", "Revenue", "ip"]]
    # Group by ip address
    group_by_ip = report_df.groupby(by="ip").agg({"Search Engine Domain" : max,
                                                  "Search Keyword": max, 
                                                  "Revenue" : sum})
    # Group by domain and keyword to bet final report
    final_report = group_by_ip.groupby(by=["Search Engine Domain",
                                           "Search Keyword"],
                                       as_index=False).sum()
    # Sort by revenue
    final_report.sort_values("Revenue", ascending=0, inplace=True)
    # Convert data frame to byte string
    csv_buffer = StringIO()
    final_report.to_csv(csv_buffer, sep = "\t", encoding="utf=8", index=False)
    # Write report to s3 bucket
    s3_client.put_object(Body=csv_buffer.getvalue(), Bucket=BUCKET,
                         Key=f"reports/{FILENAME}")
    print("Report exported.")
