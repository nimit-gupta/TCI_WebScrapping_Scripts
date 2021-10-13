#-*- coding: utf-8 -*-   
    
import requests 
import pandas as pd 
import sqlalchemy 
from bs4 import BeautifulSoup 
from datetime import date

today = date.today()


def diesel_prices():
    
    def getdata(url): 
        r = requests.get(url) 
        return r.text 

    #Link for extract html data
    htmldata = getdata("https://www.goodreturns.in/diesel-price.html") 
    soup = BeautifulSoup(htmldata, 'html.parser') 
  
    mydatastr = '' 
    result = [] 
  
    for table in soup.find_all('tr'): 
        mydatastr += table.get_text() 
  
    mydatastr = mydatastr[1:] 
    itemlist = mydatastr.split("\n\n") 
  
    for item in itemlist[:-5]: 
        result.append(item.split("\n")) 

    #Creating a dataframe
    df = pd.DataFrame(result[:-8])
    #Selecting two columns from the dataframe
    df = df.iloc[:,0:2]
    #Renaming the columns
    df.columns = ['CITY','PRICES']
    #Drop the first row from the dataframe
    df.drop(df.index[0], inplace = True)
    #Remove the rupee sign
    df['PRICES'] = df['PRICES'].str.replace('\u20b9','').astype(float)
    #Insert the date column
    df.insert(loc = 0, column = 'PRICE_DATE', value = today)
     
    #Creating the sqlalchemy engine to write the dataframe to oracle server
    engine = sqlalchemy.create_engine("oracle://xpscore:abc@124.7.209.92:1521/ORCL", echo = True)
    con = engine.connect()
    table_name = 'daily_diesel_prices_test_test'
    df.to_sql(table_name, con, if_exists = 'append', dtype={'CITY': sqlalchemy.types.VARCHAR(length=255), 'DATE': sqlalchemy.DateTime()}, index = False)

if __name__ == '__main__':
    diesel_prices()