#!/bin/sh
# Renders docs/report.md to docs/report.pdf and checks that it is exactly 2 pages (the course limit).
# Needs pandoc and Google Chrome (macOS path below, or set CHROME). figures/eval.png comes from `python3 eval/plot.py`.
set -e
cd "$(dirname "$0")"
CHROME="${CHROME:-/Applications/Google Chrome.app/Contents/MacOS/Google Chrome}"
HTML="$(mktemp -t report).html"
pandoc report.md -s -o "$HTML" --css=report.css --embed-resources --metadata pagetitle="Report"
"$CHROME" --headless=new --disable-gpu --no-pdf-header-footer --print-to-pdf=report.pdf "file://$HTML" >/dev/null 2>&1
rm -f "$HTML"
pages=$(python3 -c "import re; print(len(re.findall(rb'/Type\s*/Page[^s]', open('report.pdf','rb').read())))")
echo "report.pdf: $pages pages"
[ "$pages" = "2" ] || { echo "the report must be exactly 2 pages" >&2; exit 1; }
