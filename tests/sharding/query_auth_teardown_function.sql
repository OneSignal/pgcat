REVOKE ALL ON FUNCTION public.user_lookup(text) FROM public, md5_auth_user;
DROP FUNCTION public.user_lookup(in i_username text, out uname text, out phash text);
