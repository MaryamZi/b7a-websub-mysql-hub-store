# b7a-websub-mysql-hub-store
A MySQL based persistence store for the Ballerina WebSub Hub. 

### Running Tests
Add a config file with the following content to point to a test MySQL database.

```toml
[test.hub.db]
 url="<MYSQL_DB_URL>"
 username="<MYSQL_DB_USERNAME>"
 password="<MYSQL_DB_PASSWORD>"
 useSsl=<MYSQL_DB_USE_SSL>
```