"""从 app.long.xyz 前端 JS 重新提取 numeraire(股票)注册表 -> stocks.json"""
import json
import re
from pathlib import Path

from curl_cffi import requests

r = requests.get("https://app.long.xyz/tokens", impersonate="chrome")
chunks = re.findall(r'src="(/_next/static/chunks/[^"]+\.js)"', r.text)
s = requests.Session(impersonate="chrome")
reg = []
for c in chunks:
    t = s.get("https://app.long.xyz" + c).text
    if 's("USDG"' not in t:
        continue
    for m in re.finditer(r's\("([A-Za-z0-9x]+)","([^"]+)","(stock|stable|native|token)","(0x[0-9a-fA-F]+)"', t):
        reg.append({"symbol": m.group(1), "name": m.group(2), "kind": m.group(3), "address": m.group(4)})
    break
stocks = [x for x in reg if x["kind"] == "stock"]
print(f"{len(reg)} entries, {len(stocks)} stocks")
with open(Path(__file__).parent / "stocks.json", "w") as f:
    json.dump(reg, f, indent=1)
