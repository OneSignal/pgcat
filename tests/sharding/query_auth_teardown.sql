--- Tears down query_auth test config:
---   - Change password for sharding_user to use scram instead of md5.
---   - Drops auth query function and user.

ALTER ROLE sharding_user ENCRYPTED PASSWORD 'sharding_user' LOGIN;
DROP ROLE md5_auth_user;
