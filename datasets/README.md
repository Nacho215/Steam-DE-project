# 📚Datasets folder
When executed, the script will store raw and clean datasets in this folder into ***raw*** and ***clean*** folders:

# 📂 Raw folder
Here the script will store the following files:
- ***steam_app_list.csv***: a dataset containing id and names of ALL the apps in Steam. (See more info: [here](https://partner.steamgames.com/doc/webapi/ISteamApps#GetAppList))
- ***steam_app_data.csv***: a dataset containing details about all apps in Steam (using ids from ***steam_app_list.csv***). (See more info: [here](https://steamspy.com/api.php))

# 📁 Clean folder
Here the script will store the following files:
- ***apps***: clean and normalized dataset with all apps details.
- ***genres***: dataset with all unique genres and its ids.
- ***languages***: dataset with all unique languages and its ids.
- ***tags***: dataset with all unique tags and its ids.
- ***apps_genres***: dataset containing information about which genres have each app. This will act as an intermediate table.
- ***apps_languages***: dataset containing information about which languages have each app. This will act as an intermediate table.
- ***apps_tags***: dataset containing information about which tags have each app (and how many). This will act as an intermediate table.