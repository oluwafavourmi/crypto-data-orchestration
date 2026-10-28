# crypto-data-orchestration
this repo contains data pipeline built to perform a standard ETL(Extract, Transform, Load) task using prefect3 in python and Airflow

NOTE: I did this orchestration first in Airflow, then prefect in python.

This project was a demonstration of core data engineering principles

Here, the data moved is stored in coincodex website  (BTC USD daily price data): the pipeleine runs in this order:

Loading the website -----> scraping of contents -----> creating dataframe and doing transformations -----> saving the data to a file(CSV in this case) -----> pushing the data to a remote database(PostgreSQL is the choice here)

libraries used for this project:
- prefect
- requests
- bs4
- pandas
- sqlalchemy
- re

SEE IMAGE BELOW:

<img width="1037" height="522" alt="30 days graph" src="https://github.com/user-attachments/assets/04e50a9c-e454-49a2-8f15-5f7e0b73bc21" />




