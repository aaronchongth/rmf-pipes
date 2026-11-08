# Vendored assets

These files are compiled into the binary (`include_bytes!`), so the dashboard works air-gapped and never loads anything from the network.

| File | Upstream | Version | License | sha256 |
|---|---|---|---|---|
| `htmx-2.0.11.min.js` | https://unpkg.com/htmx.org@2.0.11/dist/htmx.min.js | 2.0.11 | 0BSD (`htmx-LICENSE`) | `d6fdc75f204e6bdefa99b69bf1e6d4ac69b8a364f77929f45c13476b4000f717` |

To upgrade:
1. Download the new build and its LICENSE.
2. Rename the file with the new version.
3. Update `HTMX_PATH`/`HTMX` in `src/ui.rs` and the `<script>` tag in `templates/base.html`.
4. Update this table.
