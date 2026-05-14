# Tiny Local Proxy Email Lookup

This project now uses:

- `index.html` for the email-first UI
- `server.py` as a tiny local proxy
- `.env` for `ROCKETREACH_API_KEY`

## Run

```bash
python server.py
```

Then open:

`http://127.0.0.1:8080`

## What It Does

- Takes a LinkedIn profile URL
- Sends it to local `POST /lookup`
- The local proxy reads `.env`
- The proxy calls RocketReach `person/search`
- The page shows a real email if RocketReach returns one
- If only teaser domains are available, the UI marks full email as unavailable

Suggested test URL:

`https://in.linkedin.com/in/omtomar`
