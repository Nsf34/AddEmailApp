import streamlit as st
import json
import datetime
import requests
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# --------------------------------------------------------------------------------
# HELPER FUNCTIONS
# --------------------------------------------------------------------------------

def get_gsheet_service_from_secrets():
    """
    Builds a Google Sheets service client from the JSON stored in st.secrets["google"]["SERVICE_ACCOUNT_JSON"].
    """
    service_account_info = st.secrets["google"]["SERVICE_ACCOUNT_JSON"]
    service_account_dict = json.loads(service_account_info)
    creds = Credentials.from_service_account_info(service_account_dict, scopes=["https://www.googleapis.com/auth/spreadsheets"])
    return build('sheets', 'v4', credentials=creds)

def fetch_sheet_data(spreadsheet_id: str, sheet_name: str, service):
    """
    Reads columns A-F from the given sheet, returning (headers, rows).
    Each row is a dict keyed by headers.
    """
    sheet = service.spreadsheets()
    range_name = f"{sheet_name}!A1:F"
    result = sheet.values().get(spreadsheetId=spreadsheet_id, range=range_name).execute()
    values = result.get('values', [])

    if not values:
        return [], []

    headers = values[0]
    data_rows = values[1:]

    rows = []
    for row_data in data_rows:
        row_data += [""] * (len(headers) - len(row_data))  # pad if shorter
        row_dict = dict(zip(headers, row_data))
        rows.append(row_dict)

    return headers, rows

def rewrite_sheet_data(spreadsheet_id: str, sheet_name: str, headers, data, service):
    """
    Overwrites the entire sheet (A1:F) with the provided headers + data.
    """
    sheet = service.spreadsheets()
    all_values = [headers] + data

    clear_range = f"{sheet_name}!A1:F"
    sheet.values().clear(spreadsheetId=spreadsheet_id, range=clear_range).execute()

    body = {"values": all_values}
    sheet.values().update(
        spreadsheetId=spreadsheet_id,
        range=clear_range,
        valueInputOption="RAW",
        body=body
    ).execute()

def append_to_sheet(spreadsheet_id: str, sheet_name: str, data, service):
    """
    Appends the given 2D list of data to the specified sheet.
    """
    sheet = service.spreadsheets()
    append_range = f"{sheet_name}!A1"
    body = {"values": data}
    sheet.values().append(
        spreadsheetId=spreadsheet_id,
        range=append_range,
        valueInputOption="RAW",
        body=body
    ).execute()

def upsert_contact_in_bigmailer(
    email: str,
    brand_id: str,
    api_key: str,
    list_id: str,
    field_values: list[dict] = None,
    validate: bool = False,
    unsubscribe_all: bool = False
):
    """
    BigMailer upsert call for a single contact to a single list.
    """
    if field_values is None:
        field_values = []

    url = f"https://api.bigmailer.io/v1/brands/{brand_id}/contacts/upsert"
    params = {"validate": str(validate).lower()}
    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "X-API-Key": api_key
    }
    data = {
        "email": email,
        "list_ids": [list_id],
        "field_values": field_values,
        "unsubscribe_all": unsubscribe_all
    }

    response = requests.post(url, headers=headers, params=params, json=data)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Upsert failed with {response.status_code}: {response.text}")


def process_contacts(
    spreadsheet_id: str,
    source_sheet_name: str,
    target_sheet_name: str,
    brand_id: str,
    api_key: str,
    allocations: dict[str, int],
    service
):
    """
    Reads unprocessed rows from 'source_sheet_name'. 
    For each list ID in 'allocations', up to N contacts are assigned. 
    A 'Successfully added...' row is appended to 'target_sheet_name',
    anything else remains in 'source_sheet_name'.
    """
    headers, rows = fetch_sheet_data(spreadsheet_id, source_sheet_name, service)
    if not rows:
        st.write(f"No rows found in '{source_sheet_name}'.")
        return

    # Filter out rows that are already 'Successfully added...'
    unprocessed = []
    for r in rows:
        if not r.get("Status", "").startswith("Successfully added"):
            unprocessed.append(r)

    processed_rows_data = []
    updated_rows_source = []
    idx = 0  # pointer into unprocessed

    # Loop through each list ID in allocations
    for list_id, count in allocations.items():
        st.write(f"Allocating {count} contacts to list: {list_id}")
        allocated = 0

        while allocated < count and idx < len(unprocessed):
            row_dict = unprocessed[idx]
            idx += 1

            email = row_dict.get("Email", "").strip()
            if not email:
                row_dict["Status"] = "Skipped: No Email"
                updated_rows_source.append(row_dict)
                st.write("Skipped row: no email provided.")
                continue

            first_name = row_dict.get("First Name", "").strip()
            last_name  = row_dict.get("Last Name", "").strip()
            tags       = row_dict.get("Tags", "").strip()

            field_values = []
            if first_name:
                field_values.append({"name": "first_name", "string": first_name})
            if last_name:
                field_values.append({"name": "last_name", "string": last_name})
            if tags:
                field_values.append({"name": "tags", "string": tags})

            try:
                upsert_contact_in_bigmailer(
                    email=email,
                    brand_id=brand_id,
                    api_key=api_key,
                    list_id=list_id,
                    field_values=field_values
                )
                success_msg = f"Successfully added on {datetime.datetime.now().strftime('%Y-%m-%d')} to {list_id}"
                row_dict["Status"] = success_msg

                row_in_order = [row_dict.get(h, "") for h in headers]
                processed_rows_data.append(row_in_order)
                st.write(f"✓ {email} => {list_id}")
            except Exception as e:
                error_msg = f"Error: {str(e)}"
                row_dict["Status"] = error_msg
                updated_rows_source.append(row_dict)
                st.write(f"✗ {email} => {error_msg}")

            allocated += 1

    # leftover unprocessed
    while idx < len(unprocessed):
        updated_rows_source.append(unprocessed[idx])
        idx += 1

    # Rebuild the source sheet with leftover + any error rows
    new_source_data = []
    for rd in updated_rows_source:
        row_list = [rd.get(h, "") for h in headers]
        new_source_data.append(row_list)

    rewrite_sheet_data(spreadsheet_id, source_sheet_name, headers, new_source_data, service)

    # Append processed
    if processed_rows_data:
        append_to_sheet(spreadsheet_id, target_sheet_name, processed_rows_data, service)
        st.write(f"Appended {len(processed_rows_data)} contacts to '{target_sheet_name}'.")

    st.write("All done. Check the sheet for updates.")


# --------------------------------------------------------------------------------
# STREAMLIT APP
# --------------------------------------------------------------------------------

def main():
    st.title("BigMailer Multi-List Allocation (Secrets-based)")

    # 1. Retrieve BigMailer brand info from secrets
    brand_id = st.secrets["bigmailer"]["BRAND_ID"]
    api_key = st.secrets["bigmailer"]["API_KEY"]

    # 2. Retrieve list IDs from secrets
    main_list_id = st.secrets["lists"]["MAIN"]
    warming1_id  = st.secrets["lists"]["WARMING1"]
    warming2_id  = st.secrets["lists"]["WARMING2"]
    warming3_id  = st.secrets["lists"]["WARMING3"]
    warming4_id  = st.secrets["lists"]["WARMING4"]
    warming5_id  = st.secrets["lists"]["WARMING5"]

    # 3. Retrieve Google Sheets info
    spreadsheet_id = st.secrets["google"]["SPREADSHEET_ID"]
    source_sheet_name = "ContactsToAdd"
    target_sheet_name = "ProcessedContacts"

    # Provide a link to your spreadsheet
    sheet_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit"
    st.markdown(f"[Open the Google Sheet]({sheet_url})")

    # Build the Google Sheets service from secrets
    service = get_gsheet_service_from_secrets()

    st.subheader("Contacts Allocation")

    # Let the user input how many for each list
    main_count = st.number_input(f"Contacts for MAIN list ({main_list_id})", min_value=0, max_value=9999, value=0)
    w1_count   = st.number_input(f"Contacts for WARMING1 ({warming1_id})", min_value=0, max_value=9999, value=0)
    w2_count   = st.number_input(f"Contacts for WARMING2 ({warming2_id})", min_value=0, max_value=9999, value=0)
    w3_count   = st.number_input(f"Contacts for WARMING3 ({warming3_id})", min_value=0, max_value=9999, value=0)
    w4_count   = st.number_input(f"Contacts for WARMING4 ({warming4_id})", min_value=0, max_value=9999, value=0)
    w5_count   = st.number_input(f"Contacts for WARMING5 ({warming5_id})", min_value=0, max_value=9999, value=0)

    total_requested = main_count + w1_count + w2_count + w3_count + w4_count + w5_count
    st.write(f"Total requested: {total_requested}")

    if st.button("Run Allocation"):
        # Build the allocations dict: {list_id: count}
        allocations = {
            main_list_id: main_count,
            warming1_id: w1_count,
            warming2_id: w2_count,
            warming3_id: w3_count,
            warming4_id: w4_count,
            warming5_id: w5_count,
        }

        st.write("Starting allocation process...")
        process_contacts(
            spreadsheet_id=spreadsheet_id,
            source_sheet_name=source_sheet_name,
            target_sheet_name=target_sheet_name,
            brand_id=brand_id,
            api_key=api_key,
            allocations=allocations,
            service=service
        )

if __name__ == "__main__":
    main()
