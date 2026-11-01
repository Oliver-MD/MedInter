# MedInter

MedInter is a web app covering registered medicinal products (SÚKL data). It provides short SPC summaries (sections 4.1–4.9) with the full official text, a list of the most prescribed substances in general practice, and a drug–drug interaction checker. The checker combines curated interaction rules, CYP/P-gp inhibitor and inducer tables, additive-effect groups, and mentions found in the SPC text.

This repository contains only the application: static HTML/JS with no dependencies, served by GitHub Pages. **No data is bundled.** The app receives its data from the page that embeds it. Opened on its own, it shows an information screen.

## Embedding

1. Embed the app in an iframe, for example `https://oliver-md.github.io/MedInter/?theme=light#/leky`.
   - Routes: `#/leky` (product list), `#/lek/<id>` (product), `#/vpl` (most prescribed in general practice, by field), `#/interakce` (interaction checker), `#/latky` and `#/latka/<code>` (substances), `#/o-databazi` (sources and method).
   - In an iframe (or with `?embed=1`) the app hides its name and theme switch; `?theme=light|dark` sets the theme.
2. The app asks its parent window for links to the data files it needs (at most 50 names per request). The embedding page answers that frame only, with `targetOrigin` set to the app's origin. Each link should be a short-lived, pre-signed URL from any object storage or CDN that supports expiring links.

```js
// app → embedding page
{ type: 'medinter:files-request', v: 1, id: 'r1', paths: ['current.json', 'rYYYYMMDD/index.json.gz', …] }

// embedding page → app
frame.contentWindow.postMessage({
  type: 'medinter:files',
  id: 'r1',                                        // same id as the request
  urls: { 'current.json': 'https://storage.example/…', … },
  expiresIn: 120,                                  // link lifetime in seconds
  theme: 'light' | 'dark',                         // optional
  shareUrl: 'https://example.org/page?state=',     // optional: prefix for share links of an interaction check
  links: { login: 'https://…', info: 'https://…' }, // optional: buttons on the lock screens
  scope: 'a1b2c3d4e5f6a7b8'                         // optional: an opaque value per signed-in account (8–64 letters,
                                                     // digits, - or _); when it changes, the app forgets the selection
                                                     // of medicines it kept in this browser (shared computers)
}, 'https://oliver-md.github.io');

// or, when the user may not use the app:
{ type: 'medinter:files', id: 'r1', error: 'login' | 'denied' | 'network' }

// embedding page → app at any time: { type: 'medinter:theme', theme: 'dark' } or { type: 'medinter:focus-search' }

// app → embedding page, optional, for the embedding page's own usage statistics (sent only to the origin that
// answered the data requests; it never contains drug names or any other content):
{ type: 'medinter:event', v: 1, name: 'view', page: 'leky' | 'lek' | 'vpl' | 'latky' | 'latka' | 'interakce' | 'o-databazi' }
{ type: 'medinter:event', v: 1, name: 'check', items: 3 }   // a new combination checked and shown for at least 2 s
```

The app never receives user credentials, tokens or API keys; it only receives links to its own data files.

## Data layout

```
current.json                      {"release": "rYYYYMMDD"}
rYYYYMMDD/meta.json.gz            build info and counts
rYYYYMMDD/index.json.gz           product list
rYYYYMMDD/substances.json.gz      active substances
rYYYYMMDD/atc.json.gz             ATC names
rYYYYMMDD/interactions.json.gz    interaction rules, CYP/P-gp table, classes
rYYYYMMDD/vpl.json.gz             most prescribed substances, grouped by field
rYYYYMMDD/products-<n>.json.gz    product variants (128 shards)
rYYYYMMDD/spc-<n>.json.gz         SPC texts and summaries (1024 shards)
```

To publish a new release, add a new release folder and then update `current.json`.

## Security

- **Content-Security-Policy** (in `index.html`): scripts may load only from this site, and no forms or plugins are allowed.
- **Messages:** the app accepts them only from the page that embeds it, and only from that page's origin.

## Sources

- SÚKL open data: Databáze léčivých přípravků, SPC texts, and eRecept prescription counts.
- EMA product information for centrally authorised products.
- FDA Table of Substrates, Inhibitors and Inducers.
- STOPP/START v3 (O'Mahony D, et al. Eur Geriatr Med. 2023;14(4):625–632, CC BY 4.0).
- ONC high-priority DDI list (Phansalkar S, et al. JAMIA 2012).
- ATC classification: WHO Collaborating Centre for Drug Statistics Methodology.

## Disclaimer

MedInter is an educational aid for study purposes only. It is not intended for decisions about the treatment of individual patients and does not replace the current SPC.

## License

The application code is under the MIT license (see [LICENSE](./LICENSE)). The Inter font is under the SIL Open Font License 1.1 (`assets/fonts/OFL.txt`).
