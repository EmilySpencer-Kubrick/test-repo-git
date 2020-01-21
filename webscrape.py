
# %%
import boto3
import datetime
import time
import requests
from bs4 import BeautifulSoup


# %%

dynamodb=boto3.resource('dynamodb', region_name = 'eu-west-2')

try:
    table= dynamodb.create_table(
        TableName='Trial',
        KeySchema=[
            {
                'AttributeName': 'time',
                'KeyType': 'HASH'  #Partition key
            },
            {
                'AttributeName': 'data',
                'KeyType': 'RANGE'  #Sort key
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'time',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'data',
                'AttributeType': 'S'
            },
    
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 1,
            'WriteCapacityUnits': 1
        }
    )
    time.sleep(5)
except Exception as e:
    print(e)

table = dynamodb.Table('Trial')

url=r'http://www.prarulebook.co.uk/rulebook/Content/Part/211136/14-01-2020'
headers={'User-Agent':	'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:72.0) Gecko/20100101 Firefox/72.0'}

r=requests.get(url=url, headers=headers)
soup = BeautifulSoup(r.text, 'html.parser')

a=soup.find('div', {'class':'div-col col3'}).text.strip()


table.put_item(
           Item={
               'time':str(datetime.datetime.now())[11:19],
               'data': a
            }
        )



#%%
data=str(datetime.datetime.now())[11:19]

s3 = boto3.resource('s3')
o = s3.Object('webscrape-output-emily', data)
o.put(Body=data)

client = boto3.client('sns', region_name= 'eu-west-2') 
response = client.publish(TopicArn='arn:aws:sns:eu-west-2:751759768047:item_in_dynamo', Message='Something has changed in DynamoDB', Subject='PRA')

# %%




