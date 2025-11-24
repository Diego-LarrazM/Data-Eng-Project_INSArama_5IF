# < Start - Transient - Extract - Load >

- To run (Linux):

1. `.../tests/Start_Transient_Extract_Load> source setup.sh`
> ... logs
> 
> ---------- < Transient Server Running > ----------
2. `(Start python venv however you prefer)`
3. `(venv) .../tests/Start_Transient_Extract_Load> python test.py`
> ... logs
> 
> ---------- < Persisted successfully N/N rows > ----------

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
