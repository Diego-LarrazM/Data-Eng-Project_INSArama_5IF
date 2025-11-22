# < SQLPersistor >

- To run (Linux):

1. `.../tests/SQLPersistor> docker compose up -d`
2. `(Start python venv however you prefer)`
3. `(venv) .../tests/SQLPersistor> python test.py`
> ... logs
> 
> ---------- < Test completed successfully > ----------


- PgAdmin connexion:

```yaml
# at localhost:5050
email: test@test.t
password: test

# Registering server
host: insm_postgres_data_warehouse
port: 5432
username: ${DW_POSTGRES_USER} # from .env
password: ${DW_POSTGRES_PASSWORD}
```
