# Overview
This project uses python to pull data from the Illustris TNG API, and then process and load it into a SQL database to be interacted with.

## Steps to Run
1) Download and run MySQL Configurator to set up a local SQL server. Remember the password you set, and modify the database connection calls in init.py and app.py to use that password
2) Run "pull-2023-12-04.ipynb" to generate the csv data files to be loaded into SQL
3) Put the generated CSV files into the "RawData" directory
4) Run init.py by typing "python init.py" in the terminal, this should initialize and load the database.
5) Install streamlit with pip: "pip install streamlit"
6) Run the GUI by typing "streamlit run app.py" in the terminal. This should then launch the application in your browser.
