#!/usr/bin/env bash

# airflow needs a home, ~/airflow is the default,
# but you can lay foundation somewhere else if you prefer
# (optional)
export AIRFLOW_HOME="~/Courses/Udacity/Data Engineering Nanodegree/capstone/project-archive/airflow"

# install from pypi using pip
pip3.7 install apache-airflow

# initialize the database
airflow initdb

# start the web server, default port is 8080
airflow webserver -p 8080

# visit localhost:8080 in the browser and enable the example dag in the home page
