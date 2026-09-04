# Okta mode

`MCP_AUTH_MODE=okta` turns PgLLens into a plain OAuth **resource server**: it
validates an access token your Okta tenant minted and serves RFC 9728 discovery.
It issues nothing, stores no credential, and registers no `/oauth/*` route.

## Prerequisite: a custom authorization server

Okta mode requires a **custom authorization server**. The built-in *Org*
authorization server mints opaque tokens with no custom audience, with no `aud`
to bind, the whole scheme is unimplementable.

Two ways a tenant has one:

- **API Access Management**, a paid add-on in production orgs, which lets the
  tenant create as many custom authorization servers as it likes.
- An **Okta Integrator Free Plan** org, which is pre-configured with a single
  custom authorization server named `default` (issuer
  `https://<tenant>.okta.com/oauth2/default`, server ID `default`). Free orgs
  cannot add more, and cannot delete this one, so use `default` as-is rather
  than creating `api://pgllens`.

If the tenant has neither, the fallbacks are mTLS at the reverse proxy or an
IP-allowlisted VPN, and password mode stays available for local use.

## Tenant setup (client side, once)

1. **Security → API → Authorization Servers.** On an Integrator Free org, open
   the pre-existing `default` server and read its **Audience** off the page
   (`api://default`). With API Access Management, **Add Authorization Server**
   instead, audience `api://pgllens`. Either way, note the issuer URI
   (`https://<tenant>.okta.com/oauth2/{default|ausXXXX}`).
2. **Scopes tab → Add Scope**, twice: `pgllens.read` and `pgllens.admin`. Leave
   *Set as a default scope* unchecked, a default scope lands in every token
   whether or not the client asked for it, which would hand `pgllens.admin` to
   clients that never requested it.
   - `pgllens.read`: the query, discovery, relationship, health, index, ERD and
     ontology tools (28 of the 31).
   - `pgllens.admin`: `get_active_sessions`, `get_blocking`, `get_query_store`.
     These three return SQL text written by *other* database users, which is why
     they are a separate grant.
3. **Access Policies tab → Add Policy, then Add Rule**, granting those scopes to
   the client applications that should reach PgLLens. This step is not optional:
   an Integrator Free org's `default` server ships with *no* access policy, and a
   client matching no policy fails the token request outright.
4. **Applications → Create App Integration:** register the MCP client. PgLLens
   does not care which grant type is used, it only ever sees the resulting
   access token. Leave **DPoP** off: a DPoP-bound token is presented as
   `Authorization: DPoP <token>` and `oauth/bearer.py` requires the `Bearer`
   scheme.

   Client credentials looks like the quickest smoke test, but on an Integrator
   Free org (2026-09) the token request fails with `invalid_grant: The NHI
   Authentication Tokens SKU is not enabled. Contact your Account Executive...`,
   even though the app integration offers the grant and discovery advertises
   `client_credentials`. The gate is a tenant SKU flag, not a config mistake --
   the error is undocumented, so do not burn time re-checking scopes or the
   access policy when you see it. It is probably also why *Client Credentials*
   is absent from the access-policy rule dialog's grant list on that plan,
   though that link is unconfirmed.

   Use authorization code instead, and use the authorization server's **Token
   Preview** tab to inspect claims without driving a redirect flow.

## PgLLens configuration

```bash
MCP_AUTH_MODE=okta
OKTA_ISSUER=https://your-tenant.okta.com/oauth2/default   # or .../oauth2/ausXXXX
OKTA_AUDIENCE=api://default                              # or api://pgllens
EXTERNAL_BASE_URL=https://pgllens.example.com
```

`OKTA_AUDIENCE` must match the authorization server's audience exactly. A
mismatch is a hard 401 on every call, by design: a token minted for a different
API in the same tenant must never be replayable against PgLLens.

## What PgLLens verifies

Every request to `/mcp` carries `Authorization: Bearer <jwt>`, and PgLLens checks:
signature (RS256 against the tenant's JWKS), `iss`, `aud`, `exp`, `nbf`, the
presence of `sub`, and the scope required by the tool being called. JWKS is
fetched at boot (the server refuses to start if it is unreachable), cached for
the `Cache-Control` max-age Okta returns, and refetched at most once a minute when
an unknown `kid` appears.

## Troubleshooting

| Symptom | Cause |
| --- | --- |
| Server exits at boot with `JWKS fetch ... failed` | Egress to the tenant is blocked, or `OKTA_ISSUER` is wrong. Fail-closed is deliberate. |
| Every call 401s | `OKTA_AUDIENCE` does not match the authorization server's audience, or the token came from the Org server. |
| `403 ... requires the pgllens.admin scope` | The client's access policy does not grant `pgllens.admin`. |
| `GET /oauth/authorize` 404s | Correct. That route only exists in password mode. |
