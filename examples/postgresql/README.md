# Project example
Here we have the most basic Meltano project example for `target-miso`.

# Getting started
If you are not familiar with Meltano yet, follow the [official guide](https://docs.meltano.com/getting-started#install-meltano) to install Meltano.

## Prepare dummy data
You can populate some dummy data using the script in `data` directory:

```bash
# only if database not exist
createdb meltano_demo

# drop and create table "users"
psql -d meltano_demo -f ./data/setup-users-table.sql

# insert some dummy user data
sh ./data/generate-users.sh -n 100 -f sql | psql -d meltano_demo
```

## Play with Meltano
You can try the data pipeline with the following commands:

```bash
meltano install
meltano run tap-postgres target-miso
```
