import pandas as pd
import os
from lxml import etree
import re

def clean_text(text: str) -> str:
    """Remove/replace common garbage characters, control chars, multiple spaces, etc."""
    if not text:
        return ""
    # Replace common replacement chars and weird unicode blocks
    text = text.replace('\ufffd', '?').replace('�', '?').replace('�', '')
    # Remove control characters (except \n and \t if you want to keep them)
    text = ''.join(c for c in text if c.isprintable() or c in '\n\t')
    # Normalize multiple spaces/newlines
    text = re.sub(r'\s+', ' ', text.strip())
    return text


def process_xml(file_path):
    rows = []
    try:
        # Try multiple common HK encodings in order of likelihood
        encodings = ['big5', 'big5hkscs', 'gb18030', 'gbk', 'utf-8', 'utf-8-sig']
        xml_text = None

        with open(file_path, 'rb') as f:
            raw_bytes = f.read()

        for enc in encodings:
            try:
                xml_text = raw_bytes.decode(enc)
                print(f"Successfully decoded {os.path.basename(file_path)} with {enc}")
                break
            except UnicodeDecodeError:
                continue

        if xml_text is None:
            # Last resort: replace errors
            xml_text = raw_bytes.decode('big5', errors='replace')
            print(f"Fallback replace decoding used for {os.path.basename(file_path)}")

        parser = etree.XMLParser(recover=True, encoding='utf-8')
        root = etree.fromstring(xml_text.encode('utf-8'), parser=parser)

        for message in root.findall("message"):
            def safe_findtext(tag):
                t = message.findtext(tag, "")
                return clean_text(t) if t else ""

            row = {
                "file": os.path.basename(file_path),
                "incident_number": safe_findtext("INCIDENT_NUMBER"),
                "heading_en": safe_findtext("INCIDENT_HEADING_EN"),
                "detail_en": safe_findtext("INCIDENT_DETAIL_EN"),
                "location_en": safe_findtext("LOCATION_EN"),
                "direction_en": safe_findtext("DIRECTION_EN"),
                "announcement_date": safe_findtext("ANNOUNCEMENT_DATE"),
                "status_en": safe_findtext("INCIDENT_STATUS_EN"),
                "near_landmark_en": safe_findtext("NEAR_LANDMARK_EN"),
                "content_en": safe_findtext("CONTENT_EN").replace("\n", " "),
            }
            rows.append(row)

    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return []

    return rows


def process_all_xml_to_csv(directory, output_csv="2024_2025_grouped_incidents_clean.csv"):
    all_rows = []

    for filename in os.listdir(directory):
        if filename.lower().endswith(".xml"):
            file_path = os.path.join(directory, filename)
            print(f"Processing {filename}...")
            all_rows.extend(process_xml(file_path))

    if not all_rows:
        print("No data found.")
        return

    print(f"Total raw rows collected: {len(all_rows)}")

    df = pd.DataFrame(all_rows)

    # Date parsing
    df['announcement_date'] = pd.to_datetime(
        df['announcement_date'],
        format='mixed',
        errors='coerce',
        dayfirst=False,
    )

    invalid = df['announcement_date'].isna().sum()
    if invalid > 0:
        print(f"Warning: {invalid} rows with invalid dates → removed")

    df = df.dropna(subset=['announcement_date'])

    df = df.sort_values(['incident_number', 'announcement_date']).reset_index(drop=True)

    def build_timeline(group):
        if group.empty:
            return "No updates available."

        group = group.drop_duplicates(subset=['announcement_date'], keep='first')

        lines = []
        prev_key = None

        for _, row in group.iterrows():
            ts = row['announcement_date'].strftime('%Y-%m-%d %H:%M:%S')
            status = clean_text(row['status_en'] or "N/A")
            detail_raw = clean_text(row['content_en'] or row['detail_en'] or "No detail provided")

            # Adjustable – 1200 should cover almost everything
            MAX_CHARS = 1200
            detail = detail_raw[:MAX_CHARS] + ("... [truncated]" if len(detail_raw) > MAX_CHARS else "")

            # Dedup key (case + space + punctuation insensitive)
            norm = detail.lower().replace(" ", "").replace(".", "").replace(",", "")
            key = (ts, norm)

            if key == prev_key:
                continue

            line = f"[{ts}] Status: {status}\n    {detail}"
            lines.append(line)
            prev_key = key

        if not lines:
            return "No meaningful updates after deduplication."

        incident_id = group['incident_number'].iloc[0] if 'incident_number' in group.columns else "Unknown"

        header = (
            f"Incident: {incident_id}\n"
            f"Location: {group['location_en'].iloc[0] or 'N/A'}\n"
            f"Heading: {group['heading_en'].iloc[0] or 'N/A'}\n"
            f"Timeline of updates (oldest to newest):\n\n"
        )

        return header + "\n\n".join(lines)

    def aggregate_group(g):
        if g.empty:
            return pd.Series({
                'incident_number': g['incident_number'].iloc[0] if 'incident_number' in g.columns else "Unknown",
                'llm_timeline': 'Empty group - no data'
            })

        return pd.Series({
            'incident_number': g['incident_number'].iloc[0],
            'first_file': g['file'].iloc[0],
            'first_announcement': g['announcement_date'].min(),
            'last_announcement': g['announcement_date'].max(),
            'duration_minutes': round(
                (g['announcement_date'].max() - g['announcement_date'].min()).total_seconds() / 60, 1
            ) if len(g) > 1 else 0,
            'llm_timeline': build_timeline(g),
            'final_status': clean_text(g['status_en'].iloc[-1]) if not g['status_en'].empty else "N/A",
            'initial_heading': clean_text(g['heading_en'].iloc[0]),
            'main_location': clean_text(g['location_en'].iloc[0]),
            'main_direction': clean_text(g['direction_en'].iloc[0]),
            'near_landmark': clean_text(g['near_landmark_en'].iloc[0]),
        })

    aggregated = (
        df.groupby('incident_number')
          .apply(aggregate_group)
          .reset_index(drop=True)
    )

    aggregated = aggregated[[
        'incident_number',
        'llm_timeline',
        'first_announcement',
        'last_announcement',
        'duration_minutes',
        'final_status',
        'initial_heading',
        'main_location',
        'main_direction',
        'near_landmark',
        'first_file'
    ]]

    # Write with utf-8-sig so Excel opens Chinese correctly
    aggregated.to_csv(output_csv, index=False, encoding='utf-8-sig')
    print(f"\nSaved cleaned results (unknown chars handled):")
    print(f"  → {output_csv}")
    print(f"  → {len(aggregated)} unique incidents")


if __name__ == "__main__":
    xml_directory = "./dataset"
    output_csv = "2024_2025_grouped_incidents_clean.csv"
    process_all_xml_to_csv(xml_directory, output_csv)