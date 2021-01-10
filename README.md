# Description du Project

Ce projet vise à acquérir des données de différentes sources de l’agglomération Lyonnaise concernant le trafic routier, la météo et le niveau de pollution constaté. Des sources sont proposées plus loin dans la section Données. En utilisant ces données, on construira un système d’alerte à la pollution.

### Informations générales:

Mon travail a été réalisé sur le repo :

<https://github.com/otman-math/big-data.git>

### Tache 1:

Collecte des données.

- les données concernant le niveau de qualité de l’air peuvent être accédées via le site <http://api.atmo-aura.fr/documentation>.
- les données concernant la méteo sont disponible sur le site <https://donneespubliques.meteofrance.fr/?fond=produit&id_produit=90&id_rubrique=32>.

### Tache 2 :

Mise en place de l'Environement technique.

# Connexion Kafka

## Producteur

kafka-console-producer --broker-list node-5:9092 --topic grp-11-matbzi-data-prod

## Consommateur

kafka-console-consumer --bootstrap-server node-5:9092 --from-beginning --topic grp-11-matbzi-data

## Spark Streaming

Les deux classes Archive.scala et Learning.scala représentent le code du deux processus de l'archivage et l'entrainement des données en batches.
Le dossier /SparkStreaming/out contient le jar généré à partir de chaque class et qui à deployer sur le cluster en utilisant la command : [ spark-submit nom_jar ]

## Hive

La commande pour ajouter la table dans hive :

> hive
> CREATE TABLE IF NOT EXISTS grp_11_matbzi_data ( date_indice DATE,
> rr6 decimal, t decimal, td decimal, tend decimal, u decimal, dd decimal, ff decimal,
> hbas decimal, insee String, nbas decimal, pmer decimal , pres decimal , valeur decimal, vv decimal,
> couleur_html String, qualificatif String, type_valeur String);

## Modèles Machine Learning

Test de plusieurs modèles pour choisir le plus optimal sur nos données, pour l'implémenter ensuite sur Spark Streaming

### References

<https://spark.apache.org/docs/latest/streaming-programming-guide.html>

<https://kafka.apache.org/documentation/>

## Authors :

- **Otman Mataich**
