# src/data_preprocessing.py
import argparse
import pandas as pd

def preprocess(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    # detect date-like columns
    date_cols = [c for c in df.columns if 'date' in c.lower() or 'time' in c.lower() or 'timestamp' in c.lower()]
    if date_cols:
        df['date'] = pd.to_datetime(df[date_cols[0]], errors='coerce')
    else:
        # fallback synthetic date
        df['date'] = pd.date_range(end=pd.Timestamp.today(), periods=len(df), freq='D')
    df = df.sort_values('date').reset_index(drop=True)

    # fill small gaps: forward then backward
    df = df.fillna(method='ffill').fillna(method='bfill')

    # try to coerce numeric columns
    for col in df.columns:
        if df[col].dtype == object:
            try:
                df[col] = pd.to_numeric(df[col])
            except Exception:
                pass
    return df

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', required=True)
    parser.add_argument('--output', required=True)
    args = parser.parse_args()

    df = pd.read_csv(args.input)
    df_clean = preprocess(df)
    df_clean.to_csv(args.output, index=False)
    print("Preprocessing done ->", args.output)

if __name__ == "__main__":
    main()
