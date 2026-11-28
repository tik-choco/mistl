# Web sites and games (`mistl upload`, `mistl web`)

Publish a static Web App or Web Game from a folder. Anyone running mistl can open it at
`http://<site>.localhost:6490/`. Their own mistl fetches the files from peers, verifies
them, and serves them on loopback only. Nothing is reachable from outside the machine,
and there is no server to run.

## Publish

```sh
mistl upload ./dist
```

```
Uploaded my-game (42 files, 38.2 MiB), version 1764300000000
Play:  http://k5q2…7a.localhost:6490/
GDM World: use this URL as the work's URL.
```

- The folder must contain `index.html` (or pass `--entry <file>`).
- The site name defaults to the folder name, or to its parent's name for `dist`, `build`,
  `out`, `public`, `www`, `site` and `html`. Override it with `--name my-game`. Names are
  lowercase letters, digits and `-`, up to 48 characters.
- Run the same command again to publish an update. The site id comes from your mistl
  identity and the name, so the URL and the players' save data (localStorage, IndexedDB)
  stay the same across versions.
- `--spa` serves the entry file for unknown paths without an extension (client-side
  routing).
- `--coi` sends `Cross-Origin-Opener-Policy` / `Cross-Origin-Embedder-Policy`. Pages need
  them for `SharedArrayBuffer`, as in Godot 4 web exports and threaded Unity builds. It is
  turned on automatically when the folder contains a `.pck` and a `.wasm` file. Turn it off
  with `--no-coi`.
- Files ending in `.br` / `.gz` are served with `Content-Encoding: br` / `gzip` and the
  type of the inner extension, as Unity's compressed builds expect.
- `--dry-run` lists what would be uploaded without storing anything.
- Dotfiles and dot-folders are skipped (except `.well-known`). Symbolic links are never
  followed.

Limits: 20,000 files, 512 MiB per file, 2 GiB per site (`[web] max_site_bytes`). Upload
reads one file at a time into memory.

### Publish from the dashboard

Open the dashboard (`mistl ui`, or the tray icon), choose **Web**, and drop the build
folder onto the page (or pick it with **Choose folder**). The page shows the file count,
the size and any skipped dotfiles. Check the name, entry file, SPA and COI options (COI
defaults to auto), then choose **Publish**. When the upload finishes, the page shows the
Play URL with **Open** and **Copy**. The **Sites** list below shows every known site with
its install progress, and lets you trust, untrust or remove it.

- The dropped folder itself is the site root: `dist/index.html` is published as
  `index.html`. Browsers do not reveal the folder's parent, so for a folder named `dist`,
  `build` and so on, type the name yourself.
- Publishing again with the same name updates the same site (same URL, saves kept).
- The same rules and limits apply as for `mistl upload`. The browser sends files one by
  one (up to 3 at a time) into `<data_dir>/web/staging/`, and mistl publishes them from
  there and then deletes the staged copy. Cancel deletes it as well, and so does an upload
  left idle for 30 minutes.
- Publishing works only in a dashboard opened on this machine, not over the LAN or a
  tunnel, the same as `web.publish`.

Your mistl keeps every file of a published site pinned and serves it to peers. Players
who installed the site serve it too, so it stays available while you are offline as long
as one of them is online.

## Play

Open the site URL in a browser on a machine with mistl running and the network on
(`mistl network on`).

1. The first time, mistl asks peers for the site's signed record.
2. A confirmation page shows the site name, the publisher's DID, the size and the date.
   Choose **Open this site**. Optionally, trust the publisher so that their other sites
   open without asking.
3. mistl downloads and pins every file, showing progress. Then the site opens. After that
   it works offline.

Updates by the same publisher install in the background. Each update switches over only
once it is complete.

```sh
mistl web ls                     # known sites: installed, trusted, size, progress
mistl web install <site> --trust # download in advance (site id or URL)
mistl web open <site>            # open in the browser
mistl web url <site>             # print the URL
mistl web trust <site|did>       # trust a site or a publisher
mistl web untrust <site|did>
mistl web rm <site>              # remove it and free its storage
mistl web status                 # gateway address, network, room
```

`http://localhost:6490/` lists the installed sites.

## Security model

- **The gateway is loopback only.** `[web] listen` must be a loopback address. Requests
  must name `<site>.localhost:<port>` (or `localhost:<port>`) in the `Host` header, which
  defeats DNS rebinding.
- **One origin per site.** Each site runs on its own origin, so sites cannot read each
  other's storage. They cannot read the dashboard either, which uses a different host and
  port and a `SameSite=Strict` cookie. The gateway never sets or reads cookies and exposes
  no mistl API. Sites cannot call the local AI API unless you add their origin to
  `ai.api_allowed_origins`.
- **Authenticity.** A site record is signed by the publisher's `did:key`. The site id is
  derived from that DID and the name, so nobody else can publish under it. Older versions
  are refused. Every file is verified by its content address.
- **Consent.** A site runs only after you confirm it, or after you trust its publisher.
  Nothing is downloaded before that. The confirmation form needs a one-time token and a
  same-origin request.
- **Browser permissions.** Camera, microphone, geolocation, USB, serial, HID, Bluetooth,
  payment and screen capture are disabled by `Permissions-Policy`. Pages served by mistl
  itself cannot be framed.
- **Privacy.** Asking for a site broadcasts its id in the `mistl-web-v1` room, so peers
  there can see which sites you open. Joining `mistl-web-v1` (enabled by default)
  also lets peers in the room request any locally stored block by CID, as in any room
  the node joins. Set `[web] enabled = false` to stay out of this room.

A trusted site is ordinary web content running in your browser, with the same powers as
any web page you visit. Trust only publishers you know.

## Configuration

```toml
[web]
enabled = true                  # host sites: gateway + room mistl-web-v1
listen = "127.0.0.1:6490"       # loopback only; keep the port stable (it is part of each site's origin)
max_site_bytes = 2147483648     # largest site published or installed here
```

`enabled` and `listen` take effect after an automatic daemon restart. Dev builds pick a
free port on first run so they don't collide with an installed release.
