import pandas as pd
import pdfplumber
import re
import os

def extract_from_pdf(pdf_path):
    """Extracts text from PDF and parses it into structured data."""
    data = []
    # Matches: Name (text), PIN (R/M + digits/spaces), and Price ($ + digits)
    pattern = re.compile(r"^(?P<owner>.*?)\s+(?P<pin>[RM]\d{3}\s?.*?\s?\d{4})\s+(?P<bid>\$\d{1,3}(?:,\d{3})*(?:\.\d{2})?)$")
    
    try:
        with pdfplumber.open(pdf_path) as pdf:
            print(f"📄 Reading {len(pdf.pages)} pages...")
            for page in pdf.pages:
                text = page.extract_text()
                if text:
                    for line in text.split('\n'):
                        line = line.strip()
                        match = pattern.match(line)
                        if match:
                            data.append(match.groupdict())
    except FileNotFoundError:
        print(f"❌ Error: {pdf_path} not found.")
        return []
        
    return data

def process_to_csv(input_file="auction_list.pdf", output_csv="auction_list.csv"):
    print(f"🚀 Starting PDF Ingestion...")
    raw_data = extract_from_pdf(input_file)
    
    if not raw_data:
        print("⚠️ No data extracted. Check filename or PDF formatting.")
        return

    df = pd.DataFrame(raw_data)
    
    # Clean up Bid column to numeric float
    df['bid'] = df['bid'].replace(r'[\$,]', '', regex=True).astype(float)
    
    df.to_csv(output_csv, index=False)
    print(f"✅ Ingestion Complete! Saved {len(df)} properties to {output_csv}")

if __name__ == "__main__":
    process_to_csv()