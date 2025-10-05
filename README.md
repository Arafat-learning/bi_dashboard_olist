#### Introduction
In this project I will create dashboard with KPIs for e-commerce giant olist using open source data in Kaggle.  

#### Set up
You first clone the project into your local machine
``` bash
git clone https://github.com/Arafat-learning/bi_dashboard_olist.git
```
Afterward build the docker image
```
docker-compose build
```
Then run the containers
```
docker-compose up
```

This project will consist of the following folders:
* **Data**: contains all the data
* **Pipeline**: processes raw data for visualizations
* **Front**: creates easy webpage to visualize the KPI's
