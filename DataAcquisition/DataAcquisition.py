from kafka import KafkaProducer
import csv
import json
from statistics import mean
from datetime import datetime
import gzip
import requests
import urllib.request
import shutil
import time
proxies = {
    "http": "http://proxy.univ-lyon1.fr:3128",
    "https": "http://proxy.univ-lyon1.fr:3128",
}
producer = KafkaProducer(bootstrap_servers='node-12:9092')
#date = datetime.datetime.now()

anneeStart = 2017
moisStart = 12
anneeFin = 2020
moisFin = 2


def load_data(file, insee, day):
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

with open('indices.json', 'a') as f:
    f.write( '[' )
#date=str(date.year)+"-"+str(date.month)+"-"+str(date.day)
while moisStart != moisFin or anneeStart != anneeFin :
    traficUrl="http://192.168.76.159/data/meteo/synop/synop."+"{:04}{:02}".format( anneeStart, moisStart  )+".csv.gz"
    print(traficUrl)

    traficPage = requests.get(traficUrl, proxies=proxies)
    print(traficPage.status_code)

    if traficPage.status_code == 200 :

        #télécharger csv.gz
        urllib.request.urlretrieve(traficUrl, "Meteo/synop."+"{:04}{:02}".format( anneeStart, moisStart  )+".csv.gz")

        #décompresser csv.gz => csv
        with gzip.open("Meteo/synop."+"{:04}{:02}".format( anneeStart, moisStart  )+".csv.gz", 'rb') as f_in:
            with open("Meteo/synop."+"{:04}{:02}".format( anneeStart, moisStart  )+".csv", 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)

        #load data of days

        for i in range(1, 28):
            data = load_data("Meteo/synop."+"{:04}{:02}".format( anneeStart, moisStart  ), "07481", "{:02}".format(  i ))
            url_day = "http://api.atmo-aura.fr/communes/69123/indices?api_token=110e7bfb6794976228d459730802f79e&date={:04}-{:02}-{:02}".format( anneeStart, moisStart, i  )

            request = requests.get( url_day, proxies = proxies )
            # test if response is OK
            if request.status_code == 200:
                # get data in json format
                data_indices_file_json = request.json()
                indices = data_indices_file_json["indices"]
                if indices is not None:
                    data["valeur"] = indices["valeur"]
                    data["couleur_html"] = indices["couleur_html"]
                    data["qualificatif"] = indices["qualificatif"]
                    data["type_valeur"] = indices["type_valeur"]
                    data["date_indice"] = indices["date"]
                    with open('indices.json', 'a') as f:
                        #f.write( json.dumps(data ,ensure_ascii=False) + ',\n' )
                        if data["qualificatif"].startswith('M'):
                            data["qualificatif"] = 0
                        else:
                            data["qualificatif"] = 1                       
                        producer.send('grp-11-matbzi-data', json.dumps(data, ensure_ascii=False ).encode('utf-8'))
                    print( json.dumps(data ,ensure_ascii=False) )
            else:
                print("Pas de données ! STATUS CODE != 200")

            print(data)
    time.sleep(5)
    moisStart = moisStart + 1
    print("hi")
    if moisStart == 13:
        moisStart = 1
        anneeStart = anneeStart +1

with open('indices.json', 'a') as f:
    f.write( ']' )


#print(load_data("synop.201911", "07481", "10"))
