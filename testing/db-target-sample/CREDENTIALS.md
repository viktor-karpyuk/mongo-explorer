# Credentials

Local testing credentials for `mex-mongo-source` (not for production use).

| Field | Value |
| --- | --- |
| Host | `localhost` |
| Port | `28000` |
| Username | `root` |
| Password | `testing-db-replication` |
| Auth DB | `admin` |

## Connection string

```
mongodb://root:testing-db-replication@localhost:28000/?authSource=admin
```

## mongosh

```
mongosh "mongodb://root:testing-db-replication@localhost:28000/?authSource=admin"
```
