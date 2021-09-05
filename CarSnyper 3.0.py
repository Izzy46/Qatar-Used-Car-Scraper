from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options
from bs4 import BeautifulSoup
import pandas as pd
import time
from slack_webhook import Slack
btg
pd.options.mode.chained_assignment = None
desired_width=400
pd.set_option('display.width', desired_width)
pd.set_option('display.max_columns',16)
pd.set_option('display.max_colwidth',100)

from twilio.rest import Client
accountSid = 'ACfac1468cd970fb1a8529ec8fa514f3a0'
authToken = '21bf784b39a50476006958ff20d070ae'
twilio_client = Client(accountSid, authToken)
from_whatsapp_number='whatsapp:+14155238886'
to_whatsapp_number='whatsapp:+32484862789'

import datetime
now = now = datetime.datetime.now()

options = Options()
options.headless = True

def qatarliving_scraper():
    driver = webdriver.Firefox(options=options)
    url = 'https://www.qatarliving.com/vehicles?page=1'
    delay = 0.5
    post_title = []
    mileage = []
    car_prices = []
    car_link = []

    try:
        driver.get(url)
        wait = WebDriverWait(driver, delay)
        wait.until(EC.presence_of_element_located((By.NAME, "og:description")))
        print("QatarLiving page is ready.")
        all_post = driver.find_elements_by_class_name("b-card.b-card-mod-h.vehicle")

        for post in all_post:
            text = post.text.split("\n")
            car_prices.append(text[-2])
            mileage.append(text[-3])
            post_title.append(text[-4])
            car_link.append(post.get_attribute('data-page-url'))

    except Exception as e:
        print("Could not load: "+url+str(e))
        return None

    df = pd.DataFrame({
        'Title': post_title,
        'Price': car_prices,
        'Mileage': mileage,
        'Link': car_link
        })

    driver.quit()
    return df

def qatarsale_scraper():
    driver = webdriver.Firefox(options=options)
    url = 'https://www.qatarsale.com/EnMain.aspx'

    try:
        driver.get(url)
        print("QatarSale page is ready.")
        html = driver.page_source
        soup = BeautifulSoup(html, 'lxml')

        gridviewcount = 2

        model = []
        make = []
        year = []
        mileage = []
        price = []
        car_link = []

        while gridviewcount < 10:
            make.append(soup.find('span', attrs={'id': 'GridView1_ctl0' + str(gridviewcount) +'_Label10'}).getText())
            model.append(soup.find('span', attrs={'id': 'GridView1_ctl0' + str(gridviewcount) +'_Label3'}).getText())
            year.append(soup.find('span', attrs={'id': 'GridView1_ctl0' + str(gridviewcount) +'_Label6'}).getText())
            mileage.append(soup.find('span', attrs={'id': 'GridView1_ctl0' + str(gridviewcount) +'_Label5'}).getText())
            price.append(soup.find('span', attrs={'id': 'GridView1_ctl0' + str(gridviewcount) +'_Label4'}).getText())
            car_link.append("https://www.qatarsale.com/EnMain.aspx")
            gridviewcount +=1
        while 35 > gridviewcount > 9:
            make.append(soup.find('span', attrs={'id': 'GridView1_ctl' + str(gridviewcount) + '_Label10'}).getText())
            model.append(soup.find('span', attrs={'id': 'GridView1_ctl' + str(gridviewcount) + '_Label3'}).getText())
            year.append(soup.find('span', attrs={'id': 'GridView1_ctl' + str(gridviewcount) + '_Label6'}).getText())
            mileage.append(soup.find('span', attrs={'id': 'GridView1_ctl' + str(gridviewcount) + '_Label5'}).getText())
            price.append(soup.find('span', attrs={'id': 'GridView1_ctl' + str(gridviewcount) + '_Label4'}).getText())
            car_link.append("https://www.qatarsale.com/EnMain.aspx")
            gridviewcount += 1

    except Exception as e:
        print("Could not load: "+url+str(e))
        return None

    df = pd.DataFrame({
        'Make' : make,
        'Model': model,
        'Price': price,
        'Mileage': mileage,
        'Year': year,
        'Link': car_link,
    })

    driver.quit()
    return df

def extract_make_model_year(df):
    df = df[df.Title.str.contains(r'[0-9]')]
    df['Year'] = df.Title.str[-4:]
    df['Year'] = pd.to_numeric(df.Year, downcast='float', errors='coerce')  # Convert to float and errors become Nan
    df['Make'] = df.Title.str.split().str.get(0)
    df['Model'] = df.Title.str.split().str.get(1)
    del df['Title']
    df = df.reindex(columns=['Make', 'Model', 'Price', 'Mileage', 'Year', 'Link'])
    return df

def convert_price_mileage_year(df):
    df['Price'] = df['Price'].astype(str)
    df['Price'] = df['Price'].str.extract('(\d+)').astype('float')
    df['Mileage'] = df['Mileage'].astype(str)
    df['Mileage'] = df['Mileage'].str.extract('(\d+)').astype('float')
    df['Year'] = df['Year'].astype('float')
    df['Make'] = df['Make'].astype(str)
    df['Make'] = df['Make'].str.lower()
    df['Model'] = df['Model'].astype(str)
    df['Model'] = df['Model'].str.lower()
    return df

def combine_and_drop_duplicates(df1, df2):
    df1['New Post'] = True
    df2['New Post'] = False
    combined_df = pd.concat([df1, df2])
    combined_df.drop_duplicates(subset=['Make', 'Model', 'Price', 'Mileage', 'Year'],inplace = True, keep='last')
    return combined_df

def calculate_mileage_z_score(df):
    df['Car Age'] = now.year + 1 - df['Year']
    df['Yearly Mileage'] = df['Mileage'] / df['Car Age']
    print(df)
    mileage_zscore = lambda x: (x - x.mean()) / x.std()
    df['Mileage Z_Score'] = df.groupby(['Make', 'Car Age'])['Mileage'].transform(mileage_zscore)
    return df

def calculate_price_z_score(df):
    price_zscore = lambda x: (x - x.mean()) / x.std()
    df['Price Z_Score'] = df.groupby(['Make', 'Car Age'])['Price'].transform(price_zscore)
    df['Price Z_Score'] = df['Price Z_Score'].astype('float')
    return df

def whatsapp_message(df):
    df = df[df['New Post']]
    df.drop(df[df['Price Z_Score'] >= -1].index, inplace=True)
    df.drop(df[df['Mileage Z_Score'] >= 0.5].index, inplace=True)
    df.dropna(subset=['Price Z_Score'], inplace=True)
    slack = Slack(url='https://hooks.slack.com/services/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')

    if df.empty:
        #slack.post(text="Nothing")
        pass
    else:
        try:
            slack.post(text='\n'.join(df['Make'].astype(str)+' '+df['Model'].astype(str)+'\n'+"Year: "+df['Year'].astype(str)+'\n'+'Mileage: '+df['Mileage'].astype(str)+'\n'+"Price: "+df['Price'].astype(str)+'\n'+'Price Z_Score: '+round(df['Price Z_Score'],2).astype(str)+"\n"+'Mileage Z_Score: '+round(df['Mileage Z_Score'],2).astype(str)+'\n'+df['Link'].astype(str)))
        except Exception as e:
            print("Could not load send to Slack. "+str(e))
            pass

def reset_database(df):
    del df['New Post']
    del df['Car Age']
    del df['Yearly Mileage']
    del df['Mileage Z_Score']
    del df['Price Z_Score']
    return df

def get_qatarsale():
    df = qatarsale_scraper()
    if df is None:
        return None
    else:
        df = convert_price_mileage_year(df)
        return df

def get_qatarliving():
    df = qatarliving_scraper()
    if df is None:
        return None
    else:
        df = extract_make_model_year(df)
        df = convert_price_mileage_year(df)
        return df

def find_deals(df):
    df.dropna(inplace=True)
    database_df = pd.read_csv('CarSnyper3_Database.csv')
    combined_df = combine_and_drop_duplicates(df, database_df)
    combined_df = calculate_mileage_z_score(combined_df)
    combined_df = calculate_price_z_score(combined_df)
    whatsapp_message(combined_df)
    combined_df = reset_database(combined_df)
    combined_df.to_csv('CarSnyper3_Database.csv', index=False)

while True:
    qatarsale_df = get_qatarsale()
    if qatarsale_df is None:
        pass
    elif not qatarsale_df.empty:
        find_deals(qatarsale_df)

    qatarliving_df = get_qatarliving()
    if qatarliving_df is None:
        pass
    elif not qatarliving_df.empty:
        find_deals(qatarliving_df)

    time.sleep(250.0 - ((time.time() - starttime) % 250.0))

