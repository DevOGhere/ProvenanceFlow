import pandas as pd
import requests
from pathlib import Path


MONTHLY_COLS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']


def download_gistemp(url: str, local_path: str) -> pd.DataFrame:
    """Download GISTEMP CSV from NASA and return parsed DataFrame."""
    Path(local_path).parent.mkdir(parents=True, exist_ok=True)
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    with open(local_path, 'wb') as f:
        f.write(response.content)
    return parse_gistemp(local_path)


def parse_gistemp(local_path: str) -> pd.DataFrame:
    """Parse a locally saved GISTEMP CSV into a clean DataFrame."""
    df = pd.read_csv(local_path, skiprows=1, na_values=['****'])
    # Drop any trailing summary rows that have non-integer Year values
    df = df[pd.to_numeric(df['Year'], errors='coerce').notna()].copy()
    df['Year'] = df['Year'].astype(int)
    return df.reset_index(drop=True)
