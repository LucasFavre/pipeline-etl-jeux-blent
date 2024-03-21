CREATE TABLE Top_jeux (
  id_game varchar(50) NOT NULL PRIMARY KEY,
  nb_ratings int NOT NULL,
  latest_rating float NOT NULL,
  oldest_rating float NOT NULL,
  avg_rating float NOT NULL
);