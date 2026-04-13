# UI Branding

The Streamlit dashboard supports optional real logo assets, but it does not require them.

If no logo files are present, the app falls back to text-only branding and still loads normally.

## Supported Files

Place the files in `ui/assets/`.

Preferred primary logo filenames:

- `logo.svg`
- `logo.png`

Optional icon filenames:

- `logo_icon.png`
- `logo_icon.svg`

## Recommendations

- Use a transparent background where possible.
- Prefer a wide logo for `logo.svg` or `logo.png`.
- Keep the icon square if you provide `logo_icon.*`.
- A raster icon around `256 x 256` is a safe default.
- Avoid large built-in margins inside the asset.

## How The App Uses Them

- `logo.svg` or `logo.png` is used for the app logo when available.
- `logo_icon.png` is preferred for the browser/page icon when available.
- If only the main logo exists and it is a raster file, the app may reuse it as the page icon.
- If only SVG assets exist, the app still shows the logo, but the browser icon may remain unset.

## Replacement Steps

1. Drop the new logo file into `ui/assets/` using one of the supported filenames.
2. Optionally add a matching icon file.
3. Reload the Streamlit app.

No code change is needed as long as the filenames stay within the supported convention above.
