# < SQLPersistor >

- To run (Linux): (pip install pytest if needed)

1. `.../tests/SQLPersistor> pytest -v --capture=sys --log-cli-level=INFO`
> ... logs

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
