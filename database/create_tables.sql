-- public.apps definition
CREATE TABLE apps (
	id_app int8 NOT NULL,
	"name" text NOT NULL,
	developer text NOT NULL,
	publisher text NOT NULL,
	owners_min int8 NULL,
	owners_max int8 NULL,
	average_forever_hs float8 NULL,
	average_2weeks_hs float8 NULL,
	median_forever_hs float8 NULL,
	median_2weeks_hs float8 NULL,
	peak_ccu_yesterday int8 NULL,
	price_usd float8 NULL,
	initial_price_usd float8 NULL,
	discount float8 NULL
);

-- public.genres definition
CREATE TABLE genres (
	id_genre int8 NOT NULL,
	genre text NULL
);

-- public.languages definition
CREATE TABLE languages (
	id_language int8 NOT NULL,
	"language" text NULL,
	"normalized_language" text NULL
);

-- public.tags definition
CREATE TABLE tags (
	id_tag int8 NOT NULL,
	tag text NULL
);

-- public.apps_genres definition
CREATE TABLE apps_genres (
	id_app int8 NULL,
	id_genre int8 NULL
);

-- public.apps_languages definition
CREATE TABLE apps_languages (
	id_app int8 NULL,
	id_language int8 NULL
);

-- public.apps_tags definition
CREATE TABLE apps_tags (
	id_app int8 NULL,
	id_tag int8 NULL,
	count int8 NULL
);

-- Add primary keys
alter table apps add primary key (id_app);
alter table genres add primary key (id_genre);
alter table languages add primary key (id_language);
alter table tags add primary key (id_tag);

-- Add foreign keys
alter table apps_genres add constraint fk_apps_genres_id_app
	foreign key (id_app) references apps (id_app);
alter table apps_genres add constraint fk_apps_genres_id_genre
	foreign key (id_genre) references genres (id_genre);
alter table apps_languages add constraint fk_apps_languages_id_app
	foreign key (id_app) references apps (id_app);
alter table apps_languages add constraint fk_apps_languages_id_language
	foreign key (id_language) references languages (id_language);
alter table apps_tags add constraint fk_apps_tags_id_app
	foreign key (id_app) references apps (id_app);
alter table apps_tags add constraint fk_apps_tags_id_tag
	foreign key (id_tag) references tags (id_tag);