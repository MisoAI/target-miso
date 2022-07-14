DROP TABLE IF EXISTS users;
CREATE TABLE users(
   id                             SERIAL PRIMARY KEY
  ,first_name                     VARCHAR(11) NOT NULL
  ,last_name                      VARCHAR(10) NOT NULL
  ,city                           VARCHAR(16) NOT NULL
  ,"state"                        VARCHAR(14) NOT NULL
  ,country                        VARCHAR(13) NOT NULL
  ,email                          VARCHAR(32) NOT NULL
  ,username                       VARCHAR(21) NOT NULL
  ,date_of_birth                  TIMESTAMP NOT NULL
  ,create_time                    TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP
);
