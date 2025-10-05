#### Introduction
In this project I created KPI dashboard for e-commerce giant olist.  
I used openly available data in Kaggle.  

#### Set up
First clone the project into your local machine
``` 
git clone https://github.com/Arafat-learning/bi_dashboard_olist.git
```
Afterwards build the the docker images from dockerfiles 
```
docker-compose build
```
Then run the containers
```
docker-compose up
```

This project consists of the following folders:
* **Data**: contains all the data
* **Notebooks** contain my drafts and thought process while building pipeline
* **Pipeline**: processes raw data to calculate all the KPI's.
* **visual**: creates easy webpage to visualize the KPI's
