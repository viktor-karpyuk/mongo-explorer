# Packaging assets

Place platform-specific app icons here:

- `icon.icns` — macOS (512×512 source recommended)
- `icon.ico` — Windows
- `icon.png` — Linux (512×512)

If these files are missing, `electron-builder` falls back to a default icon.
Generate with `electron-icon-builder` or `iconutil` from a single 1024×1024
source. Do not commit large binary assets — generate them at release time.
