import json
import requests

# Loads credentials from JSON file
with open("../reddit_credentials.json", "r", encoding="utf-8") as f:
    creds_list = json.load(f)

# Endpoint for Reddit script-type apps
TOKEN_URL = "https://www.reddit.com/api/v1/access_token"


def check_credential(creds):
    """
    Attempts to fetch an OAuth token using the provided credentials.
    Returns True if successful, False otherwise.
    """
    auth = requests.auth.HTTPBasicAuth(creds["client_id"], creds["client_secret"])
    data = {
        "grant_type": "password",
        "username": creds.get("username", ""),
        "password": creds.get("password", ""),
    }
    headers = {"User-Agent": creds.get("user_agent", "reddit-token-checker/0.1")}
    try:
        resp = requests.post(
            TOKEN_URL, auth=auth, data=data, headers=headers, timeout=10
        )
        resp.raise_for_status()
        token = resp.json().get("access_token")
        return bool(token)
    except Exception:
        return False


def main():
    results = []
    for idx, creds in enumerate(creds_list, start=1):
        ok = check_credential(creds)
        status = "OK" if ok else "FAIL"
        print(f"[{idx}] {creds.get('client_id')} -> {status}")
        results.append({"client_id": creds.get("client_id"), "status": status})


if __name__ == "__main__":
    main()
