# 🛠 Steam Data Engineering Project 📊
# 📄 Summary
This simple project consists in a Python ETL process about [Steam](https://store.steampowered.com/) apps data.

In a few words:
- It extracts data about ALL the apps in Steam using 2 API's ([Steam Web API](https://partner.steamgames.com/doc/webapi) and [SteamSpy](https://steamspy.com/about))
- It clean, process and transform that data.
- Then loads it into a PostgreSQL database hosted in the cloud ([AWS RDS](https://aws.amazon.com/rds/))
- Finally, it query that database to generate graphs and let the user explore insights in a webpage using [Streamlit](https://streamlit.io/).

# 🔀 Architecture diagram
![architecture_diagram](https://github.com/Nacho215/Steam-DE-project/blob/main/assets/architecture_diagram.jpg)

# 📁 File structure
## Folders
- ***.streamlit***: it contains a configuration file for the streamlit page.
- ***assets***: diagrams used in README files.
- ***database***: it contains a SQL script used to create database tables.
- ***datasets***: folder where raw and clean data will be stored.
- ***libs***: Python libraries (modules) used in the project.
- ***logs***: folder where project logs will be stored.
- ***src***: contains source files (Python scripts).
- ***streamlit***: it contains the Streamlit application script.
## Files
- ***.env.template*** template of the .env file that you need to fill in order to run this script.
- ***.gitignore***: list with intentionally untracked files.
- ***config_logs.conf***: logger configuration file.
- ***requirements.txt***: dependencies needed for this project.

# 🔨 Setup
## Virtual enviroment
First, create a virtual enviroment called 'venv' for this project:
```
python -m venv venv
```
Activate it (this command can be different for each OS):
```
source venv/Scripts/activate
```
Then install dependencies from requirements file:
```
pip install -r requirements.txt
```
## Configure .env file
Rename ***.env.template*** file to ***.env***.

Then complete empty values with your AWS credentials, S3 bucket name and RDS PostgreSQL information.
## Run
Now, you can run main script:
```
cd src
python main.py
```
# 🔍 Index
Here you can navigate to each section of the project, and understand how it works:
- [💾 Database](https://github.com/Nacho215/Steam-DE-project/tree/main/database)
- [📚 Datasets](https://github.com/Nacho215/Steam-DE-project/tree/main/datasets)
- [⚙ Libraries](https://github.com/Nacho215/Steam-DE-project/tree/main/libs)
- [📑 Logs](https://github.com/Nacho215/Steam-DE-project/tree/main/logs)
- [📊 Streamlit](https://github.com/Nacho215/Steam-DE-project/tree/main/streamlit)