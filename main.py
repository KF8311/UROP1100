import pandas as pd
import os
from lxml import etree

def process_xml(file_path):
    rows = []
    try:
        with open(file_path, encoding='big5', errors='replace') as f:
            xml_text = f.read()

        parser = etree.XMLParser(recover=True)
        root = etree.fromstring(xml_text.encode('utf-8'), parser=parser)

        for message in root.findall("message"):
            incident_number = message.findtext("INCIDENT_NUMBER", None)

            if not incident_number or incident_number.strip() == "":
                incident_number = "MISSING_ID_" + os.path.basename(file_path)

            row = {
                "file": os.path.basename(file_path),
                "incident_number": incident_number.strip(),
                "heading_en": message.findtext("INCIDENT_HEADING_EN", ""),
                "detail_en": message.findtext("INCIDENT_DETAIL_EN", ""),
                "location_en": message.findtext("LOCATION_EN", ""),
                "direction_en": message.findtext("DIRECTION_EN", ""),
                "announcement_date": message.findtext("ANNOUNCEMENT_DATE", ""),
                "status_en": message.findtext("INCIDENT_STATUS_EN", ""),
                "near_landmark_en": message.findtext("NEAR_LANDMARK_EN", ""),
                "content_en": message.findtext("CONTENT_EN", "").replace("\n", " ").strip(),
            }
            rows.append(row)

    except Exception as e:
        print(f"Error processing {file_path}: {e}")
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

    # Safety: ensure column exists
    if 'incident_number' not in df.columns or df['incident_number'].isna().all():
        df['incident_number'] = df['file'].str.replace('.xml', '', regex=False)

    df['announcement_date'] = pd.to_datetime(
        df['announcement_date'],
        format='mixed',
        errors='coerce',
        dayfirst=False,
    )

    invalid_dates = df['announcement_date'].isna().sum()
    if invalid_dates > 0:
        print(f"Warning: {invalid_dates} rows with unparsable dates → removed")

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
            status = (row['status_en'] or "").strip()
            detail_raw = (row['content_en'].strip() or row['detail_en'].strip() or "No detail provided")

            MAX_CHARS = 800
            detail = detail_raw[:MAX_CHARS] + ("... [truncated]" if len(detail_raw) > MAX_CHARS else "")

            norm_detail = detail.lower().replace(" ", "").replace(".", "").replace(",", "").replace("...", "")
            key = (ts, norm_detail)

            if key == prev_key:
                continue

            line = f"[{ts}] Status: {status}\n    {detail}"
            lines.append(line)

            prev_key = key

        if not lines:
            return "No meaningful updates after deduplication."

        # SAFETY: prefer group key if `incident_number` column was dropped
        if 'incident_number' in group.columns:
            incident_id = group['incident_number'].iloc[0]
        else:
            incident_id = group.name if group.name is not None else (
                group['file'].iloc[0].replace('.xml', '') if 'file' in group.columns else 'UNKNOWN'
            )

        header = f"Incident: {incident_id}\n" \
                 f"Location: {group['location_en'].iloc[0] or 'N/A'}\n" \
                 f"Heading: {group['heading_en'].iloc[0] or 'N/A'}\n" \
                 f"Timeline of updates (oldest to newest):\n\n"

        return header + "\n\n".join(lines)

    def aggregate_group(g):
        if g.empty:
            return pd.Series({'llm_timeline': 'Empty group - no data'})

        return pd.Series({
            'incident_number': g.name if g.name is not None else (
                g['file'].iloc[0].replace('.xml', '') if 'file' in g.columns else 'UNKNOWN'
            ),
            'first_file': g['file'].iloc[0],
            'first_announcement': g['announcement_date'].min(),
            'last_announcement': g['announcement_date'].max(),
            'duration_minutes': round(
                (g['announcement_date'].max() - g['announcement_date'].min()).total_seconds() / 60, 1
            ) if len(g) > 1 else 0,
            'llm_timeline': build_timeline(g),
            'final_status': g['status_en'].iloc[-1] if not g['status_en'].empty else "N/A",
            'initial_heading': g['heading_en'].iloc[0],
            'main_location': g['location_en'].iloc[0],
            'main_direction': g['direction_en'].iloc[0],
            'near_landmark': g['near_landmark_en'].iloc[0],
        })

    # FIXED: as_index=False keeps the grouping column
    aggregated = (
        df.groupby('incident_number', as_index=False)
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

    aggregated.to_csv(output_csv, index=False, encoding='utf-8-sig')
    print(f"\nSaved cleaned results:")
    print(f"  → {output_csv}")
    print(f"  → {len(aggregated)} unique incidents")


if __name__ == "__main__":
    xml_directory = "./dataset"
    output_csv = "2024_2025_grouped_incidents_clean.csv"
    process_all_xml_to_csv(xml_directory, output_csv)