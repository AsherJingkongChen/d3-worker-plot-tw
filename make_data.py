#! /usr/bin/env python3

import csv
from http.client import HTTPSConnection
import json
import os
import re


def download_wq1402_csv():
    try:
        conn = HTTPSConnection("statdb.mol.gov.tw")
        conn.request(
            "GET",
            (
                "/statiscla/webMain.aspx"
                "?sys=220&ym=8000&ymt=11310&kind=21&type=1"
                "&funid=wq1402&cycle=1&outmode=2&compmode=0&outkind=11&fldspc=1,6,"
            ),
        )
        resp = conn.getresponse()
        if resp.status == 200:
            with open("data/wq1402.csv", "wb") as fo:
                while True:
                    chunk = resp.read(4096)
                    if not chunk:
                        break
                    fo.write(chunk)
        else:
            print(f"Failure: {resp.status} ({resp.reason})")
    finally:
        conn.close()


def get_parse_date_tw():
    PAT_DATE_TW = re.compile(r"(?P<y>\d+)年\s+(?P<m>\d+)月")

    def parse_date_tw(s: str) -> tuple[int, int]:
        """ISO format: yyyy-mm"""
        d = PAT_DATE_TW.match(s)
        y = int(d["y"]) + 1911
        m = int(d["m"])
        s = f"{y:04d}-{m:02d}"
        return s

    return parse_date_tw


def make_data_json():
    parse_date_tw = get_parse_date_tw()
    with (
        open("data/wq1402.csv", mode="r", encoding="big5") as fi,
        open("data.json", mode="w", encoding="utf-8") as fo,
    ):
        ro: list[dict[str, str | int]] = []
        for ri in csv.DictReader(fi):
            x = parse_date_tw(ri["統計期"])
            ls = [
                "總計/印尼",
                "總計/馬來西亞",
                "總計/菲律賓",
                "總計/泰國",
                "總計/越南",
                "總計/蒙古",
            ]
            for l in ls:
                y = ri[l]
                y = 0 if y == "\uFF0D" else int(y)
                l = l.replace("總計/", "")
                ro.append(dict(l=l, x=x, y=y))
        json.dump(ro, fo, ensure_ascii=False, indent=2)

def check_data_json():
    with open("data.json", mode="r", encoding="utf-8") as fi:
        data = json.load(fi)
        print(f"data entries: {len(data)}")
        print(data[0:2])
    return True

if __name__ == "__main__":
    os.makedirs("data", exist_ok=True)
    download_wq1402_csv()
    make_data_json()
    assert check_data_json()
