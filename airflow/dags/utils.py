import pandas as pd
import requests
import json
#from  parameters import output_path

def extract_data_from_api():
    url = 'http://economia.awesomeapi.com.br/json/last/USD-BRL,EUR-BRL,BTC-BRL'
    response = requests.get(url)
    response = response.json()
    #df = pd.DataFrame(json.loads(response.content))

    return response

def transform_data():
    response = extract_data_from_api()
    lista = ['USDBRL','EURBRL','BTCBRL']

    dicionario = {
        'code' : [],
        'codein' : [],
        'name' : [],
        'high' : [],
        'low' : [],
        'bid' : [],
        'ask' : [],
        'timestamp' : [],
        'create_date' : [],
        'timestamp' : []
    }
    for i in lista:
        #print(response)
        #print(response[i]['code'])
        code = response[i]['code']
        codein 		= response[i]['codein']
        name  		= response[i]['name']
        high  		= response[i]['high']
        low  		= response[i]['low']
        bid  		= response[i]['bid']
        ask   		= response[i]['ask']
        timestamp  	= response[i]['timestamp']
        create_date = response[i]['create_date']
        timestamp  	= response[i]['timestamp']


        dicionario['code'].append(code)
        dicionario['codein'].append(codein)
        dicionario['name'].append(name)
        dicionario['high'].append(high)
        dicionario['low'].append(low)
        dicionario['bid'].append(bid)
        dicionario['ask'].append(ask)
        dicionario['timestamp'].append(timestamp)
        dicionario['create_date'].append(create_date)
        dicionario['timestamp'].append(timestamp)

    df = pd.DataFrame.from_dict(dicionario,orient='index')
    df = df.T

    # Dropando linhas que estão nulas baseando na coluna create_date como null
    df = df.dropna(subset=['create_date'])

    return df

def load_local_data():
    df = transform_data()
    file_path = r'C:\Users\marco\Documents\cloud\gcp\output\price_market.csv'
    df.to_csv(file_path,sep=';',index=False, mode='a')

def main():
    response = extract_data_from_api()
    df = transform_data(response)
    load_local_data(df)

