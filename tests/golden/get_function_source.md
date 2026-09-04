## pgllens · get_function_source · app_core.touch
*catalog · 2026-09-03T15:49:03Z*

- kind: `function`
- arguments: `none`
- returns: `trigger`
- language: `plpgsql`
- volatility: `volatile`
- security: `invoker`
- strict: `no`
- comment: Sets updated_at

```sql
CREATE FUNCTION app_core.touch() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN NEW.updated_at = now(); RETURN NEW; END $$
```

---
1 overload
*Next: list_functions(schema="app_core")*
