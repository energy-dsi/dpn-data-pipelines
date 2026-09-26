import os
from flask_appbuilder.security.manager import AUTH_OAUTH

AUTH_TYPE = AUTH_OAUTH
AUTH_USER_REGISTRATION = True
# MUST be a non-privileged role. FAB 4.5's _oauth_calculate_user_roles adds
# AUTH_USER_REGISTRATION_ROLE to EVERY user on EVERY login (not just as a
# fallback when no role maps), then unions it with AUTH_ROLES_MAPPING results.
# With "Admin" here, every authenticated user - including dpnreader - would
# end up with Admin. "Public" is the built-in zero-permission role, so the
# effective access is decided purely by AUTH_ROLES_MAPPING below.
AUTH_USER_REGISTRATION_ROLE = "Public"
AUTH_ROLES_SYNC_AT_LOGIN = True

# Keycloak realm base URL as reachable from inside the cluster. A single
# server_metadata_url (OIDC discovery) is used rather than pinning individual
# endpoints, because Authlib needs the jwks_uri it advertises to validate the
# ID token - a bare endpoint dict causes "Missing jwks_uri in metadata".
KEYCLOAK_INTERNAL_URL = os.environ["KEYCLOAK_INTERNAL_URL"]

OAUTH_PROVIDERS = [
    {
        # Name MUST be "keycloak" to hit Flask-AppBuilder's built-in Keycloak
        # user-info parser, which calls "<api_base_url>/openid-connect/userinfo"
        # and reads the realm roles from its "groups" claim as role_keys.
        # api_base_url therefore ends at ".../protocol/" (NOT ".../protocol/
        # openid-connect/"), or the path would double up.
        "name": "keycloak",
        "icon": "fa-key",
        "token_key": "access_token",
        "remote_app": {
            "client_id": os.environ["AIRFLOW_OIDC_CLIENT_ID"],
            "client_secret": os.environ["AIRFLOW_OIDC_CLIENT_SECRET"],
            "server_metadata_url": f"{KEYCLOAK_INTERNAL_URL}/.well-known/openid-configuration",
            "api_base_url": f"{KEYCLOAK_INTERNAL_URL}/protocol/",
            "client_kwargs": {"scope": "openid email profile"},
        },
    }
]

# Map Keycloak GROUPS to Airflow FAB roles. Keyed by GROUP name, not realm
# role name: per the comment on the "keycloak" provider above, FAB's
# get_oauth_user_info() sets role_keys = userinfo["groups"], which - per
# dpn-authentication-service's configure-keycloak.sh KC_GROUPS default
# ("dpn-admins:dpnadmin,dpn-readers:dpnreader,dpn-operators:dpnoperator") -
# holds "dpn-admins"/"dpn-readers"/"dpn-operators", never the realm role
# names themselves. Keying this map by "dpnadmin"/"dpnreader"/"dpnoperator"
# would never match, leaving every user on AUTH_USER_REGISTRATION_ROLE
# (Public / no permissions).
# Falls back to AUTH_USER_REGISTRATION_ROLE (Public) when no mapped group is
# present. "DagOperator" is a custom role (not a FAB built-in) — created
# idempotently by airflow-init-job.yaml on every deploy, since
# AUTH_ROLES_MAPPING only assigns an existing role by name, it does not
# define one.
AUTH_ROLES_MAPPING = {
    "dpnadmin": ["Op"],
    "dpnreader": ["Viewer"],
    "dpnoperator": ["DagOperator"],
}