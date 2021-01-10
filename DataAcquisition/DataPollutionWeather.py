import csv
import json
import datetime
from statistics import mean
from datetime import datetime, date, timedelta
from calendar import monthrange
import gzip
import requests
import urllib.request
import shutil
from kafka import KafkaProducer



def load_data(year_start):
    producer = KafkaProducer(bootstrap_servers='node-12:9092')
    proxies = {
                "http": "http://proxy.univ-lyon1.fr:3128",
                "https": "http://proxy.univ-lyon1.fr:3128"
            }
    today = date.today()
    one_day = timedelta(days=1)
    yesterday = today - one_day 
    
    for y in range(year_start, today.year + 1):
        for m in range (1, 13):
            if m < 10: 
                month_str = '0' + str(m)
            else:
                month_str = str(m)     
            nb_months = monthrange(y, m)[1] + 1
            load_weather(y, m)
            for d in range(1, nb_months):
                if d < 10: 
                    day_str = '0' + str(d)
                else:
                    day_str = str(d)
                data = load_indices("Meteo/synop." + str(y) + month_str, "07481", day_str) 
                url_day = "http://api.atmo-aura.fr/communes/69123/indices?api_token=110e7bfb6794976228d459730802f79e&date=" + str(y) + "-" + month_str + "-" + day_str
                request = requests.get(url_day, proxies = proxies)
                # test if response is OK
                if request.status_code == 200:
                    # get data in json format
                    data_indices_file_json = request.json()
                    indices = data_indices_file_json["indices"]
                    if indices is not None:
                        data["valeur"] = indices["valeur"]
                        data["couleur_html"] = indices["couleur_html"]
                        data["type_valeur"] = indices["type_valeur"]
                        if indices["qualificatif"].startswith('M'):
                            data["qualificatif"] = 0
                        else:
                            data["qualificatif"] = 1                       
                        
                    producer.send('grp-11-matbzi-data', json.dumps(data, ensure_ascii=False ).encode('utf-8'))

                else:
                    print("Pas de données ! STATUS CODE != 200")
                
                if y == today.year and m == today.month and d == yesterday.day:
                    return                    
    

def load_weather(year, month):
    proxies = {
                "http": "http://proxy.univ-lyon1.fr:3128",
                "https": "http://proxy.univ-lyon1.fr:3128"
            }
    
    if month < 10: 
        month = '0' + str(month) 

    trafic_url="https://donneespubliques.meteofrance.fr/donnees_libres/Txt/Synop/Archive/synop." + str(year) + str(month) + ".csv.gz"
    trafic_page = requests.get(trafic_url, proxies=proxies)
    print(trafic_page.status_code)

    if trafic_page.status_code == 200 :

        #télécharger csv.gz
        urllib.request.urlretrieve(trafic_url, "Meteo/synop." + str(year) + str(month) + ".csv.gz")

        #décompresser csv.gz => csv
        with gzip.open("Meteo/synop." + str(year) + str(month) + ".csv.gz", 'rb') as f_in:
            with open("Meteo/synop." + str(year) + str(month) + ".csv", 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)


def load_indices(file, insee, day):
    with open (file + ".csv", "r", newline="") as csv_file:
        first_line = True
        reader = csv.DictReader(csv_file, delimiter=";")
        date = file.split(".")[1] + day
        date_synop = datetime.strptime(date, "%Y%m%d").strftime("%Y-%m-%d")
        n = 0
        indices = {
            "pmer" : [], "tend" : [], "cod_tend" : [], "rr6" : [], "pres" : [], "td" : [], "t"  : [],
            "ff" : [], "dd" : [], "u" : [], "vv" : [], "nbas" : [], "hbas" : []
        }
        synop = {
            "insee" : insee,
            "date" : date_synop
        }


        for row in reader:
            if not first_line:
                if row["numer_sta"] == insee and date in row["date"]:
                    n += 1
                    for key in indices.keys():
                        if row[key] != "mq":
                            indices[key].append(float(row[key]))
                if n == 8:
                    break
            else:
                first_line = False

        for key in indices.keys():
            if len(indices[key]) != 0:
                synop[key] = round(mean(indices[key]), 2)

    return synop
        

load_data(2019)