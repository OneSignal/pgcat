--- Sets up query_auth test config:
---   - Change sharding_user password to use md5.
---   - Adds a new user and a function to perform auth_query.

ALTER ROLE sharding_user ENCRYPTED PASSWORD 'md5fa9d23e5a874c61a91bf37e1e4a9c86e'; --- sharding_user
CREATE ROLE md5_auth_user ENCRYPTED PASSWORD 'md5773def9d091e420fdc840a2c3e1b6346' LOGIN; --- secret

