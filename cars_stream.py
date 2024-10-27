import json
import pickle as pk
import re
import time

import cloudscraper
import pandas as pd
import requests
import sklearn
from bs4 import BeautifulSoup
from kafka import KafkaProducer


def scrap() :
    
    #requests
    #cloudfire protection
    scraper = cloudscraper.create_scraper(delay=10, browser='chrome')
    l=[]

    htmlpg=scraper.get('https://www.avito.ma/fr/maroc/voitures-%C3%A0_vendre?brand=13&model=dokker,duster,logan').text
    soup0= BeautifulSoup(htmlpg,'lxml')
    carac0=soup0.find_all('a',class_= 'sc-1jge648-0 eTbzNs')
    for p in carac0:
        g=p.get('href')
        if g!=None:
            htmltest=scraper.get(g).text
            soup= BeautifulSoup(htmltest,'lxml')
            obj1=soup.find_all('span',class_='sc-1x0vz2r-0 iotEHk')
            date=obj1[1].text
            if not "minute" in date:
                break
            carac=soup.find('div',class_= 'sc-1g3sn3w-4 etbZjx')
            obj0=soup.find('p',class_='sc-1x0vz2r-0 lnEFFR sc-1g3sn3w-13 czygWQ')
            l1=[]
            l2=[]
            l3=[]
            try:
                #erreur d'encoding
                u=obj0.text
                y= re.sub("[^0-9]","",u)
                try:
                    y=int(y)
                except ValueError:
                    y='prixnonspec'
                    continue
                l2+=[y]
            except AttributeError:
                l2+=['prixnonspec']
                continue
            try:
                obj1=carac.find_all('span',class_= 'sc-1x0vz2r-0 kQHNss')
                for x in obj1:
                    f=x.text
                    if len(f)<6:
                        c=f
                    elif len(f)<8:
                        h=f
                    else:
                        k=f
                #l2+=[int(re.sub("[^0-9]","",c))]
                l2+=[2024-int(re.sub("[^0-9]","",c))]
                l2+=[h]
                l2+=[k]
                obj2=carac.find_all('li',class_= 'sc-qmn92k-1 jJjeGO')
                for y in obj2:
                    c1=y.find('span',class_= 'sc-1x0vz2r-0 jZyObG')
                    c2=y.find('span',class_= 'sc-1x0vz2r-0 gSLYtF')
                    l1+=[c1.text]
                    l3+=[c2.text]
                try:
                    l2+=[l3[l1.index('Modèle')]]
                except ValueError:
                    l2+=[None]
                try:
                    l2+=[l3[l1.index('Kilométrage')]]
                except ValueError:
                    l2+=[None]
                try:
                    l2+=[l3[l1.index('Première main')]]
                except ValueError:
                    l2+=[None]
                try:
                    l2+=[l3[l1.index('Marque')]]
                except ValueError:
                    l2+=[None]
                try:
                    l2+=[l3[l1.index('État')]]
                except ValueError:
                    l2+=[None]
                try:
                    l2+=[l3[l1.index('Nombre de portes')]]
                except ValueError:
                    l2+=[None]
                try:
                    print('bi')
                    ki = l3[l1.index('Puissance fiscale')]
                    print('ki:',ki)
                    l2+=[int(re.sub("[^0-9]","",ki))]
                    #l2+=[2024-int(re.sub("[^0-9]","",ki))]
                except ValueError:
                    l2+=[None]
                try:
                    l2+=[l3[l1.index('Origine')]]
                except ValueError:
                    l2+=[None]
                l2+=[g]
                l+=[l2]
                print(l2)
            except AttributeError:
                pass
                

    fields = ['prix','Age','Carburant','Boite_a_vitesse', 'Modele','Kilometrage','Premiere main','Marque','Etat','Nombre de portes','Cheval','Origine','url']
    df = pd.DataFrame(l, columns=fields)
    #df.to_csv('avito_car_prices.csv', index=False, encoding='utf-8')
    

    df2 = df.drop(['prix','Premiere main','Marque','Etat','Nombre de portes','Origine','url'], axis=1)
    print(df2.head())


    #df2 = pd.get_dummies(df2, columns=['Carburant', 'Boite_a_vitesse', 'Modele', 'Kilometrage','Cheval'])
    df2 = pd.get_dummies(df2)

    print(df2.head())

    # Adding missing columns in case they are not present in the original data
    missing_cols = [
        'Carburant_Diesel', 'Carburant_Essence',
        'Carburant_Hybride', 'Boite_a_vitesse_Automatique',
        'Boite_a_vitesse_Manuelle', 'Modele_Dokker', 'Modele_Duster',
        'Modele_Logan', 'Kilometrage_0 - 4 999', 'Kilometrage_10 000 - 14 999',
        'Kilometrage_100 000 - 109 999', 'Kilometrage_110 000 - 119 999',
        'Kilometrage_120 000 - 129 999', 'Kilometrage_130 000 - 139 999',
        'Kilometrage_140 000 - 149 999', 'Kilometrage_15 000 - 19 999',
        'Kilometrage_150 000 - 159 999', 'Kilometrage_160 000 - 169 999',
        'Kilometrage_170 000 - 179 999', 'Kilometrage_180 000 - 189 999',
        'Kilometrage_190 000 - 199 999', 'Kilometrage_20 000 - 24 999',
        'Kilometrage_200 000 - 249 999', 'Kilometrage_25 000 - 29 999',
        'Kilometrage_250 000 - 299 999', 'Kilometrage_30 000 - 34 999',
        'Kilometrage_300 000 - 349 999', 'Kilometrage_35 000 - 39 999',
        'Kilometrage_350 000 - 399 999', 'Kilometrage_40 000 - 44 999',
        'Kilometrage_400 000 - 449 999', 'Kilometrage_45 000 - 49 999',
        'Kilometrage_450 000 - 499 999', 'Kilometrage_5 000 - 9 999',
        'Kilometrage_50 000 - 54 999', 'Kilometrage_55 000 - 59 999',
        'Kilometrage_60 000 - 64 999', 'Kilometrage_65 000 - 69 999',
        'Kilometrage_70 000 - 74 999', 'Kilometrage_75 000 - 79 999',
        'Kilometrage_80 000 - 84 999', 'Kilometrage_85 000 - 89 999',
        'Kilometrage_90 000 - 94 999', 'Kilometrage_95 000 - 99 999',
        'Kilometrage_Plus de 500 000']

    for col in missing_cols:
        if col not in df2.columns:
            df2[col] = 0

    # Reorder columns to match the desired format

    df2 = df2[['Cheval', 'Age', 'Carburant_Diesel', 'Carburant_Essence',
       'Carburant_Hybride', 'Boite_a_vitesse_Automatique',
       'Boite_a_vitesse_Manuelle', 'Modele_Dokker', 'Modele_Duster',
       'Modele_Logan', 'Kilometrage_0 - 4 999', 'Kilometrage_10 000 - 14 999',
       'Kilometrage_100 000 - 109 999', 'Kilometrage_110 000 - 119 999',
       'Kilometrage_120 000 - 129 999', 'Kilometrage_130 000 - 139 999',
       'Kilometrage_140 000 - 149 999', 'Kilometrage_15 000 - 19 999',
       'Kilometrage_150 000 - 159 999', 'Kilometrage_160 000 - 169 999',
       'Kilometrage_170 000 - 179 999', 'Kilometrage_180 000 - 189 999',
       'Kilometrage_190 000 - 199 999', 'Kilometrage_20 000 - 24 999',
       'Kilometrage_200 000 - 249 999', 'Kilometrage_25 000 - 29 999',
       'Kilometrage_250 000 - 299 999', 'Kilometrage_30 000 - 34 999',
       'Kilometrage_300 000 - 349 999', 'Kilometrage_35 000 - 39 999',
       'Kilometrage_350 000 - 399 999', 'Kilometrage_40 000 - 44 999',
       'Kilometrage_400 000 - 449 999', 'Kilometrage_45 000 - 49 999',
       'Kilometrage_450 000 - 499 999', 'Kilometrage_5 000 - 9 999',
       'Kilometrage_50 000 - 54 999', 'Kilometrage_55 000 - 59 999',
       'Kilometrage_60 000 - 64 999', 'Kilometrage_65 000 - 69 999',
       'Kilometrage_70 000 - 74 999', 'Kilometrage_75 000 - 79 999',
       'Kilometrage_80 000 - 84 999', 'Kilometrage_85 000 - 89 999',
       'Kilometrage_90 000 - 94 999', 'Kilometrage_95 000 - 99 999',
       'Kilometrage_Plus de 500 000']]

    # Now, you can use this DataFrame for prediction
    print(df2)

    filename = '/usr/local/airflow/plugins/LR_model.sav'
    with open(filename, 'rb') as model_file:
        lr = pk.load(model_file)

    predictions = lr.predict(df2)

    # Add predictions as a new column to the DataFrame
    df['Predictions'] = predictions

    df['Prediction_Status'] = ['bad' if prediction < prix else 'good' for prediction, prix in zip(df['Predictions'], df['prix'])]

    df.to_csv('/usr/local/airflow/dags/avito_car_prices.csv', index=False, encoding='utf-8')


    json_data = df.to_json(orient='records')

    return json_data



def create_kafka_producer():
    
    #Creates the Kafka producer object
    

    return KafkaProducer(bootstrap_servers=['kafka1:19092', 'kafka2:19093', 'kafka3:19094'])



if __name__ == "__main__":
    start_streaming()
