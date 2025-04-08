import json
from datetime import datetime
import re


# Read the JSON data from file
def read_json_file(filename):
    try:
        with open(filename, 'r', encoding='utf-8') as file:
            return json.load(file)
    except FileNotFoundError:
        print(f"Error: File '{filename}' not found.")
        return None
    except json.JSONDecodeError:
        print(f"Error: File '{filename}' contains invalid JSON.")
        return None


# Format date string to PostgreSQL format (YYYY-MM-DD)
def format_date(date_string):
    if not date_string:
        return "NULL"

    # Handle different date formats
    formats = [
        "%Y-%m-%dT%H:%M:%S.%f",  # ISO format with microseconds
        "%Y-%m-%d",  # Simple date format
        "%d/%m/%Y",  # Thai date format
        "%Y%m%d"  # Compact date format
    ]

    for fmt in formats:
        try:
            date_obj = datetime.strptime(date_string, fmt)
            return f"'{date_obj.strftime('%Y-%m-%d')}'"
        except ValueError:
            continue

    return f"'{date_string}'"  # Return as is if no format matches


# Clean string for SQL insertion
def clean_sql_string(s, max_length=None):
    if s is None:
        return "NULL"
    if isinstance(s, bool):
        return "TRUE" if s else "FALSE"
    if isinstance(s, (int, float)):
        return str(s)

    # Escape single quotes
    s = str(s).replace("'", "''")

    # Truncate string if max_length is specified
    if max_length and len(s) > max_length:
        s = s[:max_length]

    return f"'{s}'"


# Extract address components
def extract_address(address_data):
    if not address_data:
        return {
            'no': '',
            'moo': '',
            'village_type': '',
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
        'village_type': address_data.get('villageType', {}).get('key', '') if isinstance(
            address_data.get('villageType'), dict) else '',
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


# Generate SQL for eopen_sba table
def generate_sba_sql(json_data):
    if not json_data:
        return ""

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
    card_number = data.get('cardNumber', '')
    card_issue = data.get('cardIssueDate', {}).get('formatted', '') if data.get('cardIssueDate') else ''
    card_expiry = data.get('cardExpiryDate', {}).get('formatted', '') if data.get('cardExpiryDate') else ''

    # Additional personal info
    gender = data.get('gender', {}).get('key', '') if isinstance(data.get('gender'), dict) else ''
    birthday = data.get('birthDate', {}).get('formatted', '') if data.get('birthDate') else ''

    # Contact info
    email = data.get('email', '')
    mobile = data.get('mobileNumber', '')
    tel_no1 = data.get('telephoneNumber', '')
    tel_no2 = data.get('officeTelephoneNumber', '')
    fax_no1 = data.get('faxNumber', '')

    # Extract banking info
    bank_code = redemption_accounts.get('bankCode', '')
    bank_branch_code = redemption_accounts.get('bankBranchCode', '')
    bank_acc_type = redemption_accounts.get('bankAccountType', {}).get('key', '') if isinstance(
        redemption_accounts.get('bankAccountType'), dict) else ''
    bank_acc_no = redemption_accounts.get('bankAccountNo', '')

    # Extract dates
    created_time = json_data.get('createdTime', '')
    trans_date = datetime.now().strftime('%Y-%m-%d')

    # Extract account types and features
    account_types = json_data.get('types', [])

    # Account service types
    service_type = data.get('serviceType', {}).get('key', '') if isinstance(data.get('serviceType'), dict) else 'N'
    receive_type = data.get('receiveType', {}).get('key', '') if isinstance(data.get('receiveType'), dict) else ''
    payment_type = data.get('paymentType', {}).get('key', '') if isinstance(data.get('paymentType'), dict) else ''

    # Marketing and referral info
    mkt_id = data.get('referralId', '')

    # Account feature flags
    has_cash = 'EQUITY' in account_types
    has_credit = 'CREDIT_BALANCE' in account_types
    has_tfex = 'TFEX' in account_types
    has_bond = 'BOND' in account_types
    has_fund = 'FUND' in account_types
    has_offshore = 'OFFSHORE' in account_types

    # Prepare SQL fields and values - include all fields from the table
    fields = [
        'trans_date', 'request_time', 'app_id', 'custtype', 'accounttype',
        'ttitle', 'tname', 'tsurname', 'etitle', 'ename', 'esurname',
        'cardidtype', 'cardid', 'cardissue', 'cardexpire', 'sex', 'birthday',
        'firstaddr1', 'firstaddr2', 'firstaddr3', 'firstzipcode', 'firstctycode',
        'firsttelno1', 'firsttelno2', 'firstfaxno1', 'email1',
        'secondsame', 'secondaddr1', 'secondaddr2', 'secondaddr3',
        'secondzipcode', 'secondctycode', 'secondtelno1', 'secondtelno2',
        'secondfaxno1', 'secondfaxno2',
        'thirdsame', 'thirdaddr1', 'thirdaddr2', 'thirdaddr3',
        'thirdzipcode', 'thirdctycode', 'thirdtelno1', 'thirdtelno2',
        'thirdfaxno1', 'thirdfaxno2',
        'reverse', 'remark',
        'bankcode', 'bankbranchcode', 'bankacctype', 'bankaccno',
        'receivetype', 'paymenttype', 'servicetype', 'branch', 'mktid',
        'custgrp', 'custcode', 'docaddr',
        'appcredit_cash', 'appcredit_cb', 'appcredit_cashb',
        'appcredit_f', 'appcredit_i', 'appcredit_g',
        'eopen_type',
        'img_cardid', 'img_cardid_face', 'img_book_account', 'img_signature',
        'cash_type', 'cash_mkt', 'cash_rsk',
        'cash_bal_type', 'cash_bal_mkt', 'cash_bal_rsk',
        'credit_bal_type', 'credit_bal_mkt', 'credit_bal_rsk',
        'tfex_type', 'tfex_mkt', 'tfex_rsk',
        'bond_type', 'bond_mkt', 'bond_rsk',
        'fund_type', 'fund_mkt', 'fund_rsk',
        'offshore_type', 'offshore_mkt', 'offshore_rsk',
        'notes', 'rsk_notes', 'rsk_datetime',
        'hrs_notes', 'hrs_datetime',
        'flag_ca', 'flag_export', 'flag_export_user', 'flag_export_datetime',
        'is_active', 'entry_user', 'entry_datetime'
    ]

    # Set values based on extracted data
    values = {
        'trans_date': f"'{trans_date}'",
        'request_time': "extract(epoch from now())",
        'app_id': str(app_id),
        'custtype': "'I'",  # Individual
        'accounttype': clean_sql_string(','.join(account_types), 20),
        'ttitle': clean_sql_string(title, 30),
        'tname': clean_sql_string(th_first_name, 100),
        'tsurname': clean_sql_string(th_last_name, 100),
        'etitle': clean_sql_string(title, 30),
        'ename': clean_sql_string(en_first_name, 100),
        'esurname': clean_sql_string(en_last_name, 100),
        'cardidtype': "'C'",  # Citizen card
        'cardid': clean_sql_string(card_number, 50),
        'cardissue': clean_sql_string(card_issue, 10),
        'cardexpire': clean_sql_string(card_expiry, 10),
        'sex': clean_sql_string(gender[0] if gender else '', 1),  # First letter of gender
        'birthday': clean_sql_string(birthday, 10),
        'firstaddr1': clean_sql_string(f"{residence['no']} {residence['moo']} {residence['soi']}".strip(), 100),
        'firstaddr2': clean_sql_string(f"{residence['road']} {residence['sub_district']}".strip(), 100),
        'firstaddr3': clean_sql_string(f"{residence['district']} {residence['province']}".strip(), 100),
        'firstzipcode': clean_sql_string(residence['postal_code'], 5),
        'firstctycode': clean_sql_string(residence['country'], 3),
        'firsttelno1': clean_sql_string(mobile, 100),
        'firsttelno2': clean_sql_string(tel_no1, 100),
        'firstfaxno1': clean_sql_string(fax_no1, 100),
        'email1': clean_sql_string(email, 100),
        'secondsame': clean_sql_string(
            'Y' if data.get('mailingAddressSameAsFlag', {}).get('key') == 'Residence' else 'N', 1),
        'secondaddr1': clean_sql_string(f"{mailing['no']} {mailing['moo']} {mailing['soi']}".strip(), 100),
        'secondaddr2': clean_sql_string(f"{mailing['road']} {mailing['sub_district']}".strip(), 100),
        'secondaddr3': clean_sql_string(f"{mailing['district']} {mailing['province']}".strip(), 100),
        'secondzipcode': clean_sql_string(mailing['postal_code'], 5),
        'secondctycode': clean_sql_string(mailing['country'], 3),
        'secondtelno1': clean_sql_string(mobile, 100),  # Use same contact info
        'secondtelno2': clean_sql_string(tel_no1, 100),
        'secondfaxno1': clean_sql_string(fax_no1, 100),
        'secondfaxno2': "''",
        'thirdsame': clean_sql_string('Y' if data.get('workAddressOption', {}).get('key') == 'Residence' else 'N', 1),
        'thirdaddr1': clean_sql_string(f"{work['no']} {work['moo']} {work['soi']}".strip(), 100),
        'thirdaddr2': clean_sql_string(f"{work['road']} {work['sub_district']}".strip(), 100),
        'thirdaddr3': clean_sql_string(f"{work['district']} {work['province']}".strip(), 100),
        'thirdzipcode': clean_sql_string(work['postal_code'], 5),
        'thirdctycode': clean_sql_string(work['country'], 3),
        'thirdtelno1': clean_sql_string(tel_no2, 100),  # Use office telephone
        'thirdtelno2': "''",
        'thirdfaxno1': "''",
        'thirdfaxno2': "''",
        'reverse': "''",
        'remark': "''",
        'bankcode': clean_sql_string(bank_code, 3),
        'bankbranchcode': clean_sql_string(bank_branch_code, 4),
        'bankacctype': clean_sql_string(bank_acc_type[0] if bank_acc_type else '', 1),  # First letter of account type
        'bankaccno': clean_sql_string(bank_acc_no, 20),
        'receivetype': clean_sql_string(receive_type, 2),
        'paymenttype': clean_sql_string(payment_type, 2),
        'servicetype': clean_sql_string(service_type[0] if service_type else 'N', 1),
        'branch': "'00'",  # Default branch
        'mktid': clean_sql_string(mkt_id, 4),
        'custgrp': "'A'",  # Default customer group
        'custcode': "''",
        'docaddr': "'1'",  # Default document address
        'appcredit_cash': "''",
        'appcredit_cb': "''",
        'appcredit_cashb': "''",
        'appcredit_f': "''",
        'appcredit_i': "''",
        'appcredit_g': "''",
        'eopen_type': "'S'",  # S for System
        'img_cardid': "''",
        'img_cardid_face': "''",
        'img_book_account': "''",
        'img_signature': "''",
        'cash_type': "'Y'" if has_cash else "'N'",
        'cash_mkt': "0",
        'cash_rsk': "0",
        'cash_bal_type': "'Y'" if has_credit else "'N'",
        'cash_bal_mkt': "0",
        'cash_bal_rsk': "0",
        'credit_bal_type': "'Y'" if has_credit else "'N'",
        'credit_bal_mkt': "0",
        'credit_bal_rsk': "0",
        'tfex_type': "'Y'" if has_tfex else "'N'",
        'tfex_mkt': "0",
        'tfex_rsk': "0",
        'bond_type': "'Y'" if has_bond else "'N'",
        'bond_mkt': "0",
        'bond_rsk': "0",
        'fund_type': "'Y'" if has_fund else "'N'",
        'fund_mkt': "0",
        'fund_rsk': "0",
        'offshore_type': "'Y'" if has_offshore else "'N'",
        'offshore_mkt': "0",
        'offshore_rsk': "0",
        'notes': "''",
        'rsk_notes': "''",
        'rsk_datetime': "NULL",
        'hrs_notes': "''",
        'hrs_datetime': "NULL",
        'flag_ca': "'N'",
        'flag_export': "'N'",
        'flag_export_user': "''",
        'flag_export_datetime': "NULL",
        'is_active': "1",
        'entry_user': "'SYSTEM'",
        'entry_datetime': "now()"
    }

    # Construct SQL
    fields_str = ', '.join(fields)
    values_str = ', '.join([values.get(field, 'NULL') for field in fields])

    sql = f"INSERT INTO public.eopen_sba ({fields_str}) VALUES ({values_str});"
    return sql


# Generate SQL for eopen_stt table
def generate_stt_sql(json_data):
    if not json_data:
        return ""

    # Extract application data
    app_id = json_data.get('applicationId', 0)
    status = json_data.get('status', '')
    contract_no = json_data.get('contractNo', '')
    data = json_data.get('data', {})

    # Extract dates and times
    created_time = json_data.get('createdTime', '')
    last_updated_time = json_data.get('lastUpdatedTime', '')
    submitted_time = json_data.get('submittedTime', '')
    trans_date = datetime.now().strftime('%Y-%m-%d')

    # Extract user data
    user_data = json_data.get('user', {})
    user_id = user_data.get('userId', 0)
    cid = user_data.get('cid', '')

    # Extract personal info
    gender = data.get('gender', {}).get('key', '') if isinstance(data.get('gender'), dict) else ''
    id_card_type = data.get('idCardType', {}).get('key', '') if isinstance(data.get('idCardType'), dict) else ''
    card_issue_date = data.get('cardIssueDate', {}).get('formatted', '') if data.get('cardIssueDate') else ''
    card_expiry_date = data.get('cardExpiryDate', {}).get('formatted', '') if data.get('cardExpiryDate') else ''
    title = data.get('title', {}).get('key', '') if isinstance(data.get('title'), dict) else ''
    th_first_name = data.get('thFirstName', '')
    th_last_name = data.get('thLastName', '')
    en_first_name = data.get('enFirstName', '')
    en_last_name = data.get('enLastName', '')

    # Extract addresses
    residence = extract_address(data.get('residence', {}))
    mailing = extract_address(data.get('mailing', {}))
    contact = extract_address(data.get('contact', {}))
    work = extract_address(data.get('work', {}))

    # Extract contact info
    tel = data.get('telephoneNumber', '')
    office_tel = data.get('officeTelephoneNumber', '')
    office_tel_ext = data.get('officeTelephoneExt', '')
    mobile = data.get('mobileNumber', '')
    email = data.get('email', '')

    # Extract birth date and nationality
    birth_date = data.get('birthDate', {}).get('formatted', '') if data.get('birthDate') else ''
    nationality = data.get('nationality', {}).get('key', '') if isinstance(data.get('nationality'), dict) else data.get(
        'nationality', '')

    # Extract tax ID
    tax_id = data.get('taxId', '')

    # Extract bank account info
    redemption_accounts = data.get('otherAccountInfo', {}).get('redemptionBankAccounts', [{}])[0] if data.get(
        'otherAccountInfo', {}).get('redemptionBankAccounts') else {}
    bank_account_name = redemption_accounts.get('bankAccountName', '')

    # Extract service info
    ats_service = data.get('atsService', {}).get('key', '') if isinstance(data.get('atsService'), dict) else ''
    edvd_service = data.get('edvdService', {}).get('key', '') if isinstance(data.get('edvdService'), dict) else ''

    # Extract family info
    family_marital = data.get('familyMarital', {}).get('key', '') if isinstance(data.get('familyMarital'), dict) else ''
    family_info = data.get('familyInformation', {}) if data.get('familyInformation') else {}

    # Extract suitability data
    suit_risk_level = data.get('suitabilityRiskLevel', '')
    suit_sum = data.get('suitabilitySum', 0)

    # Extract financial info
    financial_info = data.get('financialInformation', {}) if data.get('financialInformation') else {}
    occupation = financial_info.get('occupation', {}).get('key', '') if isinstance(financial_info.get('occupation'),
                                                                                   dict) else ''
    business = financial_info.get('business', {}).get('key', '') if isinstance(financial_info.get('business'),
                                                                               dict) else ''
    company = financial_info.get('company', '')
    monthly_income = financial_info.get('monthlyIncome', {}).get('key', '') if isinstance(
        financial_info.get('monthlyIncome'), dict) else ''

    # Extract income sources
    income_sources = financial_info.get('incomeSources', [])
    income_source_country = financial_info.get('incomeSourceCountry', {}).get('key', '') if isinstance(
        financial_info.get('incomeSourceCountry'), dict) else ''
    asset_value = financial_info.get('assetValue', {}).get('key', '') if isinstance(financial_info.get('assetValue'),
                                                                                    dict) else ''

    # Extract investment objectives
    invest_objectives = data.get('investmentObjectives', [])

    # Extract political person info
    political_person = data.get('politicalPerson', {}).get('key', '') if isinstance(data.get('politicalPerson'),
                                                                                    dict) else ''
    political_position = data.get('politicalPosition', '')

    # Extract account types
    types = ','.join(json_data.get('types', []))

    # Extract verification type
    verifi_type = json_data.get('verificationType', '')

    # Extract suitability questions and answers
    suitability_questionnaire = data.get('suitabilityQuestionnaire', {})

    # Prepare SQL fields and values
    fields = [
        'trans_date', 'request_time', 'flag_process', 'app_id', 'status', 'types', 'verifi_type',
        'contract_no', 'created_time', 'last_updated_time', 'submitted_time',
        'u_userid', 'u_cid', 'gender', 'id_card_type', 'card_issue_date', 'card_expiry_date',
        't_title', 't_fname', 't_lname', 'e_title', 'e_fname', 'e_lname',
        'tel', 'office_tel', 'office_tel_ext', 'mobile', 'email', 'birth_date', 'nationality', 'taxid',
        'mail_same_flag', 'mail_no', 'mail_moo', 'mail_village', 'mail_building', 'mail_floor',
        'mail_soi', 'mail_road', 'mail_sub_district', 'mail_district', 'mail_province',
        'mail_country', 'mail_postal',
        'cont_same_flag', 'cont_no', 'cont_moo', 'cont_village', 'cont_building', 'cont_floor',
        'cont_soi', 'cont_road', 'cont_sub_district', 'cont_district', 'cont_province',
        'cont_country', 'cont_postal',
        'resi_no', 'resi_moo', 'resi_village', 'resi_building', 'resi_floor',
        'resi_soi', 'resi_road', 'resi_sub_district', 'resi_district', 'resi_province',
        'resi_country', 'resi_postal',
        'work_no', 'work_moo', 'work_village', 'work_building', 'work_floor',
        'work_soi', 'work_road', 'work_sub_district', 'work_district', 'work_province',
        'work_country', 'work_postal',
        'census_flag',
        'redemp_bank_code', 'redemp_bank_branch_code', 'redemp_bank_account_no', 'redemp_bank_account_name',
        'ats_service', 'edvd_service', 'edvd_bank_code', 'edvd_bank_branch_code', 'edvd_bank_account_no',
        'family_marital', 'family_title', 'family_fname', 'family_lname', 'family_phone',
        'family_id_card_type', 'family_card_no', 'family_card_expiry_date',
        'finan_occupation', 'finan_business', 'finan_company', 'finan_monthly_income',
        'finan_inc_source1', 'finan_inc_source2', 'finan_inc_source3', 'finan_inc_source4',
        'finan_inc_source5', 'finan_inc_source6', 'finan_inc_source7', 'finan_inc_source7_other',
        'finan_inc_source_country', 'finan_asset_value',
        'invest_id1', 'invest_label1', 'invest_id2', 'invest_label2',
        'invest_id3', 'invest_label3', 'invest_id4', 'invest_label4',
        'invest_id5', 'invest_label5', 'invest_id6', 'invest_label6',
        'invest_id7', 'invest_label7', 'invest_id8', 'invest_label8',
        'political_person', 'political_position',
        'suit_sum', 'suit_risk_level',
        'zip_file', 'zip_password', 'flag_conv', 'is_active',
        'mailing_method', 'referralid',
        'entry_user', 'entry_datetime'
    ]

    # Extract income sources (up to 7)
    inc_sources = [source.get('key', '') if isinstance(source, dict) else '' for source in income_sources[:7]]
    while len(inc_sources) < 7:
        inc_sources.append('')

    # Extract investment objectives (up to 8)
    invest_objectives_data = []
    for obj in invest_objectives[:8]:
        invest_objectives_data.append({
            'id': obj.get('id', ''),
            'label': obj.get('label', '')
        })
    while len(invest_objectives_data) < 8:
        invest_objectives_data.append({'id': '', 'label': ''})

    # Set values based on extracted data
    values = {
        'trans_date': f"'{trans_date}'",
        'request_time': "extract(epoch from now())",
        'flag_process': "'N'",
        'app_id': str(app_id),
        'status': clean_sql_string(status, 20),
        'types': clean_sql_string(types, 200),
        'verifi_type': clean_sql_string(verifi_type, 50),
        'contract_no': clean_sql_string(contract_no, 20),
        'created_time': clean_sql_string(created_time, 30),
        'last_updated_time': clean_sql_string(last_updated_time, 30),
        'submitted_time': clean_sql_string(submitted_time, 30),
        'u_userid': str(user_id),
        'u_cid': clean_sql_string(cid, 30),
        'gender': clean_sql_string(gender, 10),
        'id_card_type': clean_sql_string(id_card_type, 30),
        'card_issue_date': clean_sql_string(card_issue_date, 10),
        'card_expiry_date': clean_sql_string(card_expiry_date, 10),
        't_title': clean_sql_string(title, 20),
        't_fname': clean_sql_string(th_first_name, 70),
        't_lname': clean_sql_string(th_last_name, 70),
        'e_title': clean_sql_string(title, 20),
        'e_fname': clean_sql_string(en_first_name, 50),
        'e_lname': clean_sql_string(en_last_name, 50),
        'tel': clean_sql_string(tel, 30),
        'office_tel': clean_sql_string(office_tel, 30),
        'office_tel_ext': clean_sql_string(office_tel_ext, 30),
        'mobile': clean_sql_string(mobile, 30),
        'email': clean_sql_string(email, 50),
        'birth_date': clean_sql_string(birth_date, 10),
        'nationality': clean_sql_string(nationality, 5),
        'taxid': clean_sql_string(tax_id, 30),
        'mail_same_flag': clean_sql_string(
            data.get('mailingAddressSameAsFlag', {}).get('key', '') if isinstance(data.get('mailingAddressSameAsFlag'),
                                                                                  dict) else '', 20),
        'mail_no': clean_sql_string(mailing['no'], 100),
        'mail_moo': clean_sql_string(mailing['moo'], 100),
        'mail_village': clean_sql_string(mailing['village'], 100),
        'mail_building': clean_sql_string(mailing['building'], 100),
        'mail_floor': clean_sql_string(mailing['floor'], 100),
        'mail_soi': clean_sql_string(mailing['soi'], 100),
        'mail_road': clean_sql_string(mailing['road'], 100),
        'mail_sub_district': clean_sql_string(mailing['sub_district'], 100),
        'mail_district': clean_sql_string(mailing['district'], 100),
        'mail_province': clean_sql_string(mailing['province'], 100),
        'mail_country': clean_sql_string(mailing['country'], 10),
        'mail_postal': clean_sql_string(mailing['postal_code'], 10),
        'cont_same_flag': clean_sql_string(
            data.get('contactAddressSameAsFlag', {}).get('key', '') if isinstance(data.get('contactAddressSameAsFlag'),
                                                                                  dict) else '', 20),
        'cont_no': clean_sql_string(contact['no'], 100),
        'cont_moo': clean_sql_string(contact['moo'], 100),
        'cont_village': clean_sql_string(contact['village'], 100),
        'cont_building': clean_sql_string(contact['building'], 100),
        'cont_floor': clean_sql_string(contact['floor'], 100),
        'cont_soi': clean_sql_string(contact['soi'], 100),
        'cont_road': clean_sql_string(contact['road'], 100),
        'cont_sub_district': clean_sql_string(contact['sub_district'], 100),
        'cont_district': clean_sql_string(contact['district'], 100),
        'cont_province': clean_sql_string(contact['province'], 100),
        'cont_country': clean_sql_string(contact['country'], 10),
        'cont_postal': clean_sql_string(contact['postal_code'], 10),
        'resi_no': clean_sql_string(residence['no'], 100),
        'resi_moo': clean_sql_string(residence['moo'], 100),
        'resi_village': clean_sql_string(residence['village'], 100),
        'resi_building': clean_sql_string(residence['building'], 100),
        'resi_floor': clean_sql_string(residence['floor'], 100),
        'resi_soi': clean_sql_string(residence['soi'], 100),
        'resi_road': clean_sql_string(residence['road'], 100),
        'resi_sub_district': clean_sql_string(residence['sub_district'], 100),
        'resi_district': clean_sql_string(residence['district'], 100),
        'resi_province': clean_sql_string(residence['province'], 100),
        'resi_country': clean_sql_string(residence['country'], 10),
        'resi_postal': clean_sql_string(residence['postal_code'], 10),
        'work_no': clean_sql_string(work['no'], 100),
        'work_moo': clean_sql_string(work['moo'], 100),
        'work_village': clean_sql_string(work['village'], 100),
        'work_building': clean_sql_string(work['building'], 100),
        'work_floor': clean_sql_string(work['floor'], 100),
        'work_soi': clean_sql_string(work['soi'], 100),
        'work_road': clean_sql_string(work['road'], 100),
        'work_sub_district': clean_sql_string(work['sub_district'], 100),
        'work_district': clean_sql_string(work['district'], 100),
        'work_province': clean_sql_string(work['province'], 100),
        'work_country': clean_sql_string(work['country'], 10),
        'work_postal': clean_sql_string(work['postal_code'], 10),
        'redemp_bank_code': clean_sql_string(redemption_accounts.get('bankCode', ''), 3),
        'redemp_bank_branch_code': clean_sql_string(redemption_accounts.get('bankBranchCode', ''), 5),
        'redemp_bank_account_no': clean_sql_string(redemption_accounts.get('bankAccountNo', ''), 20),
        'redemp_bank_account_name': clean_sql_string(bank_account_name, 200),
        'ats_service': clean_sql_string(ats_service, 1),
        'edvd_service': clean_sql_string(edvd_service, 1),
        'edvd_bank_code': clean_sql_string(data.get('edvdBankCode', ''), 3),
        'edvd_bank_branch_code': clean_sql_string(data.get('edvdBankBranchCode', ''), 5),
        'edvd_bank_account_no': clean_sql_string(data.get('edvdBankAccountNo', ''), 20),
        'family_marital': clean_sql_string(family_marital, 20),
        'family_title': clean_sql_string(
            family_info.get('title', {}).get('key', '') if isinstance(family_info.get('title'), dict) else '', 30),
        'family_fname': clean_sql_string(family_info.get('firstName', ''), 70),
        'family_lname': clean_sql_string(family_info.get('lastName', ''), 70),
        'family_phone': clean_sql_string(family_info.get('phone', ''), 30),
        'family_id_card_type': clean_sql_string(
            family_info.get('idCardType', {}).get('key', '') if isinstance(family_info.get('idCardType'), dict) else '',
            30),
        'family_card_no': clean_sql_string(family_info.get('cardNo', ''), 30),
        'family_card_expiry_date': clean_sql_string(
            family_info.get('cardExpiryDate', {}).get('formatted', '') if family_info.get('cardExpiryDate') else '',
            20),
        'finan_occupation': clean_sql_string(occupation, 100),
        'finan_business': clean_sql_string(business, 100),
        'finan_company': clean_sql_string(company, 200),
        'finan_monthly_income': clean_sql_string(monthly_income, 100),
        'finan_inc_source1': clean_sql_string(inc_sources[0], 100),
        'finan_inc_source2': clean_sql_string(inc_sources[1], 100),
        'finan_inc_source3': clean_sql_string(inc_sources[2], 100),
        'finan_inc_source4': clean_sql_string(inc_sources[3], 100),
        'finan_inc_source5': clean_sql_string(inc_sources[4], 100),
        'finan_inc_source6': clean_sql_string(inc_sources[5], 100),
        'finan_inc_source7': clean_sql_string(inc_sources[6], 100),
        'finan_inc_source7_other': clean_sql_string(financial_info.get('incomeSourceOther', ''), 100),
        'finan_inc_source_country': clean_sql_string(income_source_country, 50),
        'finan_asset_value': clean_sql_string(asset_value, 50),
        'invest_id1': clean_sql_string(invest_objectives_data[0]['id'], 50),
        'invest_label1': clean_sql_string(invest_objectives_data[0]['label'], 50),
        'invest_id2': clean_sql_string(invest_objectives_data[1]['id'], 50),
        'invest_label2': clean_sql_string(invest_objectives_data[1]['label'], 50),
        'invest_id3': clean_sql_string(invest_objectives_data[2]['id'], 50),
        'invest_label3': clean_sql_string(invest_objectives_data[2]['label'], 50),
        'invest_id4': clean_sql_string(invest_objectives_data[3]['id'], 50),
        'invest_label4': clean_sql_string(invest_objectives_data[3]['label'], 50),
        'invest_id5': clean_sql_string(invest_objectives_data[4]['id'], 50),
        'invest_label5': clean_sql_string(invest_objectives_data[4]['label'], 50),
        'invest_id6': clean_sql_string(invest_objectives_data[5]['id'], 50),
        'invest_label6': clean_sql_string(invest_objectives_data[5]['label'], 50),
        'invest_id7': clean_sql_string(invest_objectives_data[6]['id'], 50),
        'invest_label7': clean_sql_string(invest_objectives_data[6]['label'], 50),
        'invest_id8': clean_sql_string(invest_objectives_data[7]['id'], 50),
        'invest_label8': clean_sql_string(invest_objectives_data[7]['label'], 50),
        'political_person': clean_sql_string(political_person, 10),
        'political_position': clean_sql_string(political_position, 100),
        'suit_sum': str(data.get('suitabilitySum', 0)),
        'suit_risk_level': clean_sql_string(suit_risk_level, 10),
        'zip_file': "'N'",
        'zip_password': "''",
        'flag_conv': "'N'",
        'is_active': "1",
        'mailing_method': clean_sql_string(
            data.get('mailingMethod', {}).get('key', '') if isinstance(data.get('mailingMethod'), dict) else '', 20),
        'census_flag': "'N'",
        'referralid': clean_sql_string(data.get('referralId', ''), 10),
        'entry_user': "'SYSTEM'",
        'entry_datetime': "now()"
    }
    
    # Construct SQL
    fields_str = ', '.join(fields)
    values_str = ', '.join([values.get(field, 'NULL') for field in fields])
    
    sql = f"INSERT INTO public.eopen_stt ({fields_str}) VALUES ({values_str});"
    return sql

# Main function
def main():
    # File paths
    json_file = 'data.json'
    output_file = 'sql_inserts.sql'
    
    # Read JSON data
    json_data = read_json_file(json_file)
    if not json_data:
        return
    
    # Generate SQL statements
    sba_sql = generate_sba_sql(json_data)
    stt_sql = generate_stt_sql(json_data)
    
    # Write SQL to file
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write("-- SQL generated from JSON data\n")
        f.write("-- Generated on: " + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + "\n\n")
        
        f.write("-- Insert into eopen_sba table\n")
        f.write(sba_sql + "\n\n")
        
        f.write("-- Insert into eopen_stt table\n")
        f.write(stt_sql + "\n")
    
    print(f"SQL statements have been written to {output_file}")
    print("I will done.")

if __name__ == "__main__":
    main()
