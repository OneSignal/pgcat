CREATE OR REPLACE FUNCTION public.user_lookup(in i_username text, out uname text, out phash text)
RETURNS record AS $$
BEGIN
    SELECT usename, passwd FROM pg_catalog.pg_shadow
    WHERE usename = i_username INTO uname, phash;
    RETURN;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

REVOKE ALL ON FUNCTION public.user_lookup(text) FROM public, md5_auth_user;
GRANT EXECUTE ON FUNCTION public.user_lookup(text) TO md5_auth_user;
