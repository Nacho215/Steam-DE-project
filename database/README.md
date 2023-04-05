# 💾 Database
Stores clean, transformed data from the ETL process about Steam games.

Database engine is PostgreSQL and is hosted in AWS RDS (See more info: [here](https://aws.amazon.com/rds/))

In this folder is located the SQL script used to create the tables and its contraints.

# 🔧 Tables structure
This ER diagram shows diferent tables in the database, and relationship betweeen them:
![ER_diagram](https://raw.githubusercontent.com/Nacho215/Steam-DE-project/main/database/ER_diagram.png?token=GHSAT0AAAAAABZWLQ5CYIDJQT4B6W3NTV7SZBNYFBA)

You can view this diagram here: [Interactive ER diagram](https://dbdiagram.io/d/642db0f85758ac5f172708ab)

Let's explain each table structure:

## apps
Is the main table, contains most information about Steam Apps. It has the following columns:

- ***id_app***: id used by steam to uniquely identify each app. Primary key.
- ***name***: name of the app.
- ***developer***: name of the developer.
- ***publisher***: name of the publisher.
- ***owners_min***: minimum number of app owners. Owner: user that has the game on his library.
- ***owners_max***: maximum number of app owners. Owner: user that has the game on his library.
- ***average_forever_hs***: average playtime since March 2009. In hours.
- ***average_2weeks_hs***: average playtime in the last two weeks. In hours.
- ***median_forever_hs***: median playtime since March 2009. In hours.
- ***median_2weeks_hs***: median playtime in the last two weeks. In hours.
- ***peak_ccu_yesterday***: peak concurrent users yesterday. 
- ***price_usd***: current US price in dollars.
- ***initial_price_usd***: original US price in dollars.
- ***discount***: current discount in percents.

## genres
This table contains all genres from all apps in apps table. Columns:

- ***id_genre***: id that uniquely identifies a genre. Primary key.
- ***genre***: name of the genre.

## languages
This table contains all languages from all apps in apps table. Columns:

- ***id_language***: id that uniquely identifies a language. Primary key.
- ***language***: name of the language.

## tags
This table contains all tags from all apps in apps table. Columns:

- ***id_tag***: id that uniquely identifies a tag. Primary key.
- ***tag***: name of the tag.

## apps_genres
This is an intermediate table between **apps** and **genres**. This enable a many to many relationship. Columns:

- ***id_app***: id of the app. Foreign key of **id_app** on **apps** table.
- ***id_genre***: genre of the app. Foreign key of **id_genre** on **genres** table.

## apps_languages
This is an intermediate table between **apps** and **languages**. This enable a many to many relationship. Columns:

- ***id_app***: id of the app. Foreign key of **id_app** on **apps** table.
- ***id_language***: language of the app. Foreign key of **id_language** on **languages** table.

## apps_tags
This is an intermediate table between **apps** and **tags**. This enable a many to many relationship. Columns:

- ***id_app***: id of the app. Foreign key of **id_app** on **apps** table.
- ***id_tag***: tag of the app. Foreign key of **id_tag** on **tags** table.
- ***count***: number of times users voted for that tag on that app.