{{/*
Browser-facing base URL of the oauth2-proxy fronting this UI, without a
trailing slash. Everything the browser is redirected to (redirect_url,
whitelist_domains) is derived from this, so it must match exactly what
users type in the address bar - including the port. Mirrors
oauth2-proxy.externalUrl in dpn-health-monitoring-service's
charts/oauth2-proxy/_helpers.tpl.
*/}}
{{- define "self-service.oauth2ProxyExternalUrl" -}}
{{- required "oauth2Proxy.externalUrl is required: the browser-facing URL of the oauth2-proxy fronting this UI, e.g. http://10.0.0.10" .Values.oauth2Proxy.externalUrl | trimSuffix "/" -}}
{{- end -}}

{{/*
host[:port] of oauth2ProxyExternalUrl - oauth2-proxy's whitelist_domains
takes a bare host:port, not a URL.
*/}}
{{- define "self-service.oauth2ProxyExternalHost" -}}
{{- include "self-service.oauth2ProxyExternalUrl" . | trimPrefix "https://" | trimPrefix "http://" -}}
{{- end -}}

{{/*
Browser-facing Keycloak base URL - where the user is sent to log in. This is
Keycloak's own LoadBalancer address, NOT its in-cluster name: the browser
cannot resolve *.svc.cluster.local from outside the cluster's own VNet DNS.
*/}}
{{- define "self-service.keycloakPublicUrl" -}}
{{- required "oauth2Proxy.keycloak.publicUrl is required: the browser-facing Keycloak base URL" .Values.oauth2Proxy.keycloak.publicUrl | trimSuffix "/" -}}
{{- end -}}

{{/*
host[:port] of the browser-facing Keycloak URL - needed because logging out
chains /oauth2/sign_out?rd=<Keycloak end-session URL>, and that rd points at
a different host than this proxy - see whitelist_domains in
oauth2-proxy-configmap.yaml.
*/}}
{{- define "self-service.keycloakPublicHost" -}}
{{- include "self-service.keycloakPublicUrl" . | trimPrefix "https://" | trimPrefix "http://" -}}
{{- end -}}

{{/*
In-cluster Keycloak base URL, used for the server-to-server calls only
(token redeem, JWKS).
*/}}
{{- define "self-service.keycloakInternalUrl" -}}
{{- .Values.oauth2Proxy.keycloak.internalUrl | trimSuffix "/" -}}
{{- end -}}

{{- define "self-service.keycloakRealmUrl" -}}
{{- printf "%s/realms/%s" (include "self-service.keycloakPublicUrl" .) .Values.oauth2Proxy.keycloak.realm -}}
{{- end -}}

{{- define "self-service.keycloakInternalRealmUrl" -}}
{{- printf "%s/realms/%s" (include "self-service.keycloakInternalUrl" .) .Values.oauth2Proxy.keycloak.realm -}}
{{- end -}}

{{/*
The issuer oauth2-proxy validates the token's `iss` claim against - a string
comparison, never fetched, since discovery is skipped. Defaults to the
internal URL: the request that mints the token is this pod's own
token-redeem call over the in-cluster name, so that's the `iss` Keycloak
stamps into it even though the user logged in on the public address. Set
oauth2Proxy.keycloak.issuerUrl explicitly only if KC_HOSTNAME is ever pinned
on Keycloak.
*/}}
{{- define "self-service.issuerUrl" -}}
{{- default (include "self-service.keycloakInternalUrl" .) .Values.oauth2Proxy.keycloak.issuerUrl | trimSuffix "/" -}}
{{- end -}}

{{- define "self-service.issuerRealmUrl" -}}
{{- printf "%s/realms/%s" (include "self-service.issuerUrl" .) .Values.oauth2Proxy.keycloak.realm -}}
{{- end -}}
