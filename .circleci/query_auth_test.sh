#!/bin/bash
# Query auth test

set -e
set -o xtrace

export LOCAL_IP=$(hostname -i)

# In config file we have this commented:
#  [general]
#  ...
#  auth_query = "SELECT * FROM public.user_lookup('$1');"
#  auth_query_user = "md5_auth_user"
#  auth_query_password = "secret"
#  ...

# Before (sets up auth_query in postgres and pgcat)
PGDATABASE=shard0 PGPASSWORD=postgres exec_in_servers postgres tests/sharding/query_auth_setup.sql
PGDATABASE=shard0 PGPASSWORD=postgres exec_in_servers postgres tests/sharding/query_auth_setup_function.sql

sed -i 's/^# auth_query/auth_query/' .circleci/pgcat.toml

# TEST_WRONG_AUTH_QUERY BEGIN
#    When auth_query fails...
PGDATABASE=shard0 \
    PGPASSWORD=postgres \
    psql -e -h 127.0.0.1 -p 5432 -U postgres -c "REVOKE ALL ON FUNCTION public.user_lookup(text) FROM public, md5_auth_user;"

kill -SIGHUP $(pgrep pgcat) # Reload config
sleep 0.2

#    ... we can still connect.
echo "When query_auth_config is wrong, we fall back to passwords set in cleartext."
psql -U sharding_user -h 127.0.0.1 -p 6432 -c 'SELECT 1'

# After
PGDATABASE=shard0 \
    PGPASSWORD=postgres \
    psql -e -h 127.0.0.1 -p 5432 -U postgres -c "GRANT EXECUTE ON FUNCTION public.user_lookup(text) TO md5_auth_user;"
# TEST_WRONG_AUTH_QUERY END

# TEST_AUTH_QUERY BEGIN
#    When no passwords are specified in config file...
sed -i 's/^password =/# password =/' .circleci/pgcat.toml
kill -SIGHUP $(pgrep pgcat) # Reload config
sleep 0.2

#    ... we can still connect
echo "When no passwords are specified in config file, and query_auth is set, we can still connect"
psql -U sharding_user -h 127.0.0.1 -p 6432 -c 'SELECT 1'
# TEST_AUTH_QUERY END

# TEST_PASSWORD_CHANGE BEGIN
#    When we change the password of a user in postgres...
PGDATABASE=shard0 \
    PGPASSWORD=postgres \
    psql -e -h 127.0.0.1 -p 5432 -U postgres \
    -c "ALTER USER sharding_user WITH ENCRYPTED PASSWORD 'md5b47a59331e93a520d20e90fc8a3355a4'; --- another_sharding_password"

#    ... we can connect using the new password
echo "When we change pass in postgres the new hash is fetched after a connection error."
PGPASSWORD=another_sharding_password psql -U sharding_user -h "${LOCAL_IP}" -p 6432 -c 'SELECT 1'
# TEST_PASSWORD_CHANGE END

# After
PGDATABASE=shard0 PGPASSWORD=postgres exec_in_servers postgres tests/sharding/query_auth_teardown_function.sql
PGDATABASE=shard0 PGPASSWORD=postgres exec_in_servers postgres tests/sharding/query_auth_teardown.sql
sed -i 's/^auth_query/# auth_query/' .circleci/pgcat.toml
sed -i 's/^# password =/password =/' .circleci/pgcat.toml

kill -SIGHUP $(pgrep pgcat)
sleep 0.2
