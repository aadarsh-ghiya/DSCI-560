import requests
from pathlib import Path

webpage = "https://www.cnbc.com/world/?region=world"

def main() -> None:
	root = Path(__file__).resolve().parents[1]

	raw_dir = root/"data"/"raw_data"
	processed_dir = root/"data"/"processed_data"
	raw_dir.mkdir(parents=True, exist_ok=True)
	processed_dir.mkdir(parents=True, exist_ok=True) 
	out_file = raw_dir/"web_data.html"
	print("[INFO] Fetching:", webpage)

	headers = {
		"User-Agent": ( 
			"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
			"AppleWebKit/537.36 (KHTML, like Gecko) "
			"Chrome/120.0.0.0 Safari/537.36"
		),
		"Accept-Language": "en-US,en;q=0.9",
	}
	resp = requests.get(webpage, headers=headers, timeout=30)
	resp.raise_for_status()

	out_file.write_bytes(resp.content)

	print(f"[OK] Saved raw HTML to: {out_file}") 
	print(f"[OK] HTTP status: {resp.status_code}, bytes: {len(resp.content):,}")
if __name__ == "__main__": 
	main()
