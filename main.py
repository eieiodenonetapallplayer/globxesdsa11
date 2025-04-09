import json
from datetime import datetime
import psycopg2

# Database connection configuration
DB_CONFIG = {
    'host': '127.0.0.1',
    'database': 'eopen', 
    'user': 'postgres', 
    'password': 'ramil999',
    'port': '5432'
}


def connect_db():
    """Create a connection to PostgreSQL database"""
    try:
        connection = psycopg2.connect(**DB_CONFIG)
        print("Successfully connected to PostgreSQL database")
        return connection
    except Exception as e:
        print(f"Error connecting to PostgreSQL: {e}")
        return None


# Read the JSON data from file
def read_json_file(filename):
    try:
        with open(filename, 'r', encoding='utf-8') as file:
            data = json.load(file)
            print(f"Successfully loaded JSON data from {filename}")
            return data
    except FileNotFoundError:
        print(f"Error: File '{filename}' not found.")
        return None
    except json.JSONDecodeError:
        print(f"Error: File '{filename}' contains invalid JSON.")
        return None


# Extract address components from JSON structure
def extract_address(address_data):
    if not address_data:
        return {
            'no': '',
            'moo': '',
            'village': '',
            'building': '',
            'floor': '',
            'soi': '',
            'road': '',
            'sub_district': '',
            'district': '',
            'province': '',
            'country': '',
            'postal_code': ''
        }

    return {
        'no': address_data.get('no', ''),
        'moo': address_data.get('moo', ''),
        'village': address_data.get('village', ''),
        'building': address_data.get('building', ''),
        'floor': address_data.get('floor', ''),
        'soi': address_data.get('soi', ''),
        'road': address_data.get('road', ''),
        'sub_district': address_data.get('subDistrict', ''),
        'district': address_data.get('district', ''),
        'province': address_data.get('province', ''),
        'country': address_data.get('country', ''),
        'postal_code': address_data.get('postalCode', '')
    }


def format_date(date_string):
    """Convert various date formats to YYYY-MM-DD or return None if invalid"""
    if not date_string:
        return None

    # Try different date formats
    formats = [
        "%Y-%m-%dT%H:%M:%S.%f",  # ISO format with microseconds
        "%Y-%m-%d",  # Simple date format
        "%d/%m/%Y",  # Thai date format
        "%Y%m%d"  # Compact date format
    ]

    for fmt in formats:
        try:
            date_obj = datetime.strptime(date_string, fmt)
            return date_obj.strftime('%Y-%m-%d')
        except ValueError:
            continue

    return date_string  # Return as is if no format matches


def insert_sba_data(json_data, connection):
    """Insert data into eopen_sba table using parameterized query"""
    if not json_data or not connection:
        return False

    try:
        cursor = connection.cursor()

        # Extract application data
        app_id = json_data.get('applicationId', 0)
        data = json_data.get('data', {})

        # Extract addresses
        residence = extract_address(data.get('residence', {}))
        mailing = extract_address(data.get('mailing', {}))
        work = extract_address(data.get('work', {}))

        # Extract bank account info
        redemption_accounts = data.get('otherAccountInfo', {}).get('redemptionBankAccounts', [{}])[0] if data.get(
            'otherAccountInfo', {}).get('redemptionBankAccounts') else {}

        # Extract personal info
        title = data.get('title', {}).get('key', '') if isinstance(data.get('title'), dict) else ''
        th_first_name = data.get('thFirstName', '')
        th_last_name = data.get('thLastName', '')
        en_first_name = data.get('enFirstName', '')
        en_last_name = data.get('enLastName', '')

        # Card info
        card_id_type = data.get('cardIdType', {}).get('key', '') if isinstance(data.get('cardIdType'), dict) else ''
        card_number = data.get('cardNumber', '')
        card_issue = format_date(
            data.get('cardIssueDate', {}).get('formatted', '') if data.get('cardIssueDate') else '')
        card_expiry = format_date(
            data.get('cardExpiryDate', {}).get('formatted', '') if data.get('cardExpiryDate') else '')

        # Additional personal info
        gender = data.get('gender', {}).get('key', '') if isinstance(data.get('gender'), dict) else ''
        birthday = format_date(data.get('birthDate', {}).get('formatted', '') if data.get('birthDate') else '')

        # Contact info
        email = data.get('email', '')
        mobile = data.get('mobileNumber', '')
        tel_no1 = data.get('telephoneNumber', '')
        tel_no2 = data.get('officeTelephoneNumber', '')
        fax_no1 = data.get('faxNumber', '')

        # Construct address strings
        first_addr1 = f"{residence['no']} {residence['moo']} {residence['road']}".strip()
        first_addr2 = f"{residence['sub_district']} {residence['district']}".strip()
        first_addr3 = residence['province'].strip()
        first_zipcode = residence['postal_code'].strip()
        first_ctycode = residence['country'].strip()

        # Extract banking info
        bank_code = redemption_accounts.get('bankCode', '')
        bank_branch_code = redemption_accounts.get('bankBranchCode', '')
        bank_acc_type = redemption_accounts.get('bankAccountType', {}).get('key', '') if isinstance(
            redemption_accounts.get('bankAccountType'), dict) else ''
        bank_acc_no = redemption_accounts.get('bankAccountNo', '')

        # Extract account types and features
        account_types = json_data.get('types', [])
        account_type_str = ','.join(account_types)

        # Account service types
        service_type = data.get('serviceType', {}).get('key', '') if isinstance(data.get('serviceType'), dict) else 'N'
        receive_type = data.get('receiveType', {}).get('key', '') if isinstance(data.get('receiveType'), dict) else ''
        payment_type = data.get('paymentType', {}).get('key', '') if isinstance(data.get('paymentType'), dict) else ''

        # Marketing and referral info
        mkt_id = data.get('referralId', '')

        # Account feature flags
        has_cash = 'Y' if 'EQUITY' in account_types else 'N'
        has_credit = 'Y' if 'CREDIT_BALANCE' in account_types else 'N'
        has_tfex = 'Y' if 'TFEX' in account_types else 'N'
        has_bond = 'Y' if 'BOND' in account_types else 'N'
        has_fund = 'Y' if 'FUND' in account_types else 'N'
        has_offshore = 'Y' if 'OFFSHORE' in account_types else 'N'

        # Current date for transaction date
        trans_date = datetime.now().date()

        # Use parameterized query
        insert_query = """
        INSERT INTO public.eopen_sba (
            trans_date, request_time, app_id, custtype, accounttype,
            ttitle, tname, tsurname, etitle, ename, esurname,
            cardidtype, cardid, cardissue, cardexpire, sex, birthday,
            firstaddr1, firstaddr2, firstaddr3, firstzipcode, firstctycode,
            firsttelno1, firsttelno2, firstfaxno1, email1,
            bankcode, bankbranchcode, bankacctype, bankaccno,
            receivetype, paymenttype, servicetype, mktid,
            cash_type, credit_bal_type, tfex_type, bond_type, fund_type, offshore_type,
            is_active, entry_user, entry_datetime
        ) VALUES (
            %s, EXTRACT(epoch FROM now()), %s, 'I', %s,
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s,
            1, 'SYSTEM', now()
        )
        """

        # Prepare values tuple for the query
        values = (
            trans_date, app_id, account_type_str,
            title, th_first_name, th_last_name, '', en_first_name, en_last_name,
            card_id_type[:1] if card_id_type else '', card_number, card_issue, card_expiry,
            gender[:1] if gender else '', birthday,
            first_addr1, first_addr2, first_addr3, first_zipcode, first_ctycode,
            tel_no1, tel_no2, fax_no1, email,
            bank_code, bank_branch_code,
            bank_acc_type[:1] if bank_acc_type else '',
            bank_acc_no,
            receive_type, payment_type,
            service_type[:1] if service_type else 'N',
            mkt_id,
            has_cash, has_credit, has_tfex, has_bond, has_fund, has_offshore
        )

        # Execute query with parameters
        cursor.execute(insert_query, values)
        connection.commit()
        print(f"Successfully inserted data into eopen_sba for app_id: {app_id}")
        return True

    except Exception as e:
        print(f"Error inserting data into eopen_sba: {e}")
        connection.rollback()
        return False
    finally:
        if cursor:
            cursor.close()


def insert_stt_data(json_data, connection):
    """Insert data into eopen_stt table using parameterized query"""
    if not json_data or not connection:
        return False

    try:
        cursor = connection.cursor()

        # Extract application data
        app_id = json_data.get('applicationId', 0)
        status = json_data.get('status', '')
        types = ','.join(json_data.get('types', []))
        data = json_data.get('data', {})

        # Extract personal info
        title_t = data.get('title', {}).get('key', '') if isinstance(data.get('title'), dict) else ''
        title_e = data.get('titleEn', {}).get('key', '') if isinstance(data.get('titleEn'), dict) else ''
        th_first_name = data.get('thFirstName', '')
        th_last_name = data.get('thLastName', '')
        en_first_name = data.get('enFirstName', '')
        en_last_name = data.get('enLastName', '')

        # Extract contact information
        mobile = data.get('mobileNumber', '')
        email = data.get('email', '')

        # Extract ID card information
        card_type = data.get('cardIdType', {}).get('key', '') if isinstance(data.get('cardIdType'), dict) else ''
        card_no = data.get('cardNumber', '')
        card_issue_date = format_date(
            data.get('cardIssueDate', {}).get('formatted', '') if data.get('cardIssueDate') else '')
        card_expiry_date = format_date(
            data.get('cardExpiryDate', {}).get('formatted', '') if data.get('cardExpiryDate') else '')

        # Extract timestamps
        created_time = json_data.get('createdTime', '')
        submitted_time = json_data.get('submittedTime', '')
        last_updated_time = json_data.get('lastUpdatedTime', '')

        # Current date for transaction date
        trans_date = datetime.now().date()

        # Prepare parameterized query
        insert_query = """
        INSERT INTO public.eopen_stt (
            trans_date, request_time, flag_process, app_id, status, types,
            t_title, t_fname, t_lname, e_title, e_fname, e_lname,
            mobile, email, id_card_type, card_issue_date, card_expiry_date,
            created_time, last_updated_time, submitted_time,
            is_active, entry_user, entry_datetime
        ) VALUES (
            %s, EXTRACT(epoch FROM now()), 'N', %s, %s, %s,
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s,
            1, 'SYSTEM', now()
        )
        """

        values = (
            trans_date, app_id, status, types,
            title_t, th_first_name, th_last_name, title_e, en_first_name, en_last_name,
            mobile, email, card_type, card_issue_date, card_expiry_date,
            created_time, last_updated_time, submitted_time
        )

        cursor.execute(insert_query, values)
        connection.commit()
        print(f"Successfully inserted data into eopen_stt for app_id: {app_id}")
        return True

    except Exception as e:
        print(f"Error inserting data into eopen_stt: {e}")
        connection.rollback()
        return False
    finally:
        if cursor:
            cursor.close()


def main():
    # Read JSON file
    json_data = read_json_file('data.json')
    if not json_data:
        print("Failed to read JSON file")
        return

    # Connect to database
    connection = connect_db()
    if not connection:
        print("Failed to connect to database")
        return

    try:
        # Insert data into tables
        sba_success = insert_sba_data(json_data, connection)
        stt_success = insert_stt_data(json_data, connection)

        if sba_success and stt_success:
            print("Data successfully inserted into both tables")
        else:
            print("Error occurred while inserting data")

    finally:
        if connection:
            connection.close()
            print("Database connection closed")


if __name__ == "__main__":
    main()
