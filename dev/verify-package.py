import json, zipfile, hashlib, sys, os, glob

pkg = sys.argv[1] if len(sys.argv) > 1 else max(
    glob.glob(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "dist", "*.dbxp")),
    key=os.path.getmtime,
)
print("package:", os.path.basename(pkg))
zf = zipfile.ZipFile(pkg)
names = set(zf.namelist())
checksums = json.loads(zf.read("checksums.json").decode("utf-8"))

files = checksums.get("files") if isinstance(checksums, dict) else None
entries = files if isinstance(files, dict) else checksums
covered = {k.replace("\\", "/") for k in entries}
required = names - {"checksums.json", "signature.json"}

print("in package :", sorted(names))
print("declared   :", sorted(covered))
print("undeclared :", sorted(required - covered) or "none")
print("ghost      :", sorted(covered - required) or "none")

bad = []
for name in sorted(required):
    declared = entries.get(name) or entries.get(name.replace("/", "\\"))
    if not declared:
        continue
    digest = hashlib.sha256(zf.read(name)).hexdigest()
    expected = str(declared).replace("sha256:", "").strip().lower()
    if digest != expected:
        bad.append((name, expected, digest))
print("hash mismatches:", bad or "none")
sys.exit(0 if not (required - covered) and not bad else 1)
