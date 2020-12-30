import requests


def http_req(url, methon, headers, datas, timeout=30):
    sess = requests.Session()
    sess.headers.update(headers)
    methon = methon.upper()
    try:
        if methon == "GET":
            rsp = sess.get(url, params=datas, timeout=timeout)
        elif methon == "POST":
            rsp = sess.post(url, data=datas, timeout=timeout)
    except:
        return "Request failed.", "", "", ""

    return "", rsp.status_code, rsp.headers, rsp.text
