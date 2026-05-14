from __future__ import annotations

import csv
import io
import os
import pathlib
import re
import tempfile
from datetime import datetime
from urllib.parse import urlparse

import requests

ROOT = pathlib.Path(__file__).resolve().parent
ENV_PATH = ROOT / '.env'
INDEX_PATH = ROOT / 'index.html'
DEFAULT_BASE_URL = 'https://api.rocketreach.co/api/v2'
DEFAULT_SEARCH_URL = f'{DEFAULT_BASE_URL}/person/search'
DEFAULT_LOOKUP_URL = f'{DEFAULT_BASE_URL}/profile-company/lookup'
SEARCH_URL = DEFAULT_SEARCH_URL
LOOKUP_URL = DEFAULT_LOOKUP_URL
OUTPUT_COLUMNS = [
    'Date',
    'Company Name',
    'Position',
    'Job Link',
    'Submitted',
    'HR Name',
    'HR Position',
    'HR Profile Link',
    'HR Email',
    'HR Secondary Email',
    'HR Email Preview',
    'HR Contact',
    'HR Contact Preview',
    'RocketReach Status',
]

HR_LINK_ALIASES = (
    'hr link',
    'hr profile',
    'hr profile link',
    'linkedin',
    'linkedin url',
    'profile link',
)

HR_NAME_ALIASES = (
    'hr name',
    'name',
)

HR_POSITION_ALIASES = (
    'hr position',
    'recruiter position',
)

JOB_POSITION_ALIASES = (
    'position',
    'job position',
)

COMPANY_NAME_ALIASES = (
    'company name',
    'company',
)

JOB_LINK_ALIASES = (
    'job link',
)

DATE_ALIASES = (
    'date',
)

SUBMITTED_ALIASES = (
    'submitted',
)


def load_env(env_path: pathlib.Path = ENV_PATH):
    env = {}
    if env_path.exists():
        for line in env_path.read_text(encoding='utf-8').splitlines():
            line = line.strip()
            if not line or line.startswith('#') or '=' not in line:
                continue
            key, value = line.split('=', 1)
            env[key.strip()] = value.strip()
    return env


def build_rocketreach_headers(env_path: pathlib.Path = ENV_PATH) -> dict:
    env = load_env(env_path)
    api_key = env.get('ROCKETREACH_API_KEY', '').strip()
    if not api_key:
        raise RuntimeError('ROCKETREACH_API_KEY not found in .env.')
    return {
        'Api-Key': api_key,
        'Content-Type': 'application/json',
    }


def build_rocketreach_runtime(env_path: pathlib.Path = ENV_PATH) -> dict:
    env = load_env(env_path)
    base_url = env.get('ROCKETREACH_BASE_URL', '').strip().rstrip('/') or DEFAULT_BASE_URL
    timeout_raw = env.get('ROCKETREACH_TIMEOUT_SECONDS', '').strip()
    try:
        timeout_seconds = max(1, int(timeout_raw)) if timeout_raw else 30
    except ValueError:
        timeout_seconds = 30

    use_env_proxy_raw = env.get('ROCKETREACH_USE_ENV_PROXY', '').strip().lower()
    use_env_proxy = use_env_proxy_raw in {'1', 'true', 'yes', 'on'}
    return {
        'base_url': base_url,
        'search_url': f'{base_url}/person/search',
        'lookup_url': f'{base_url}/profile-company/lookup',
        'timeout_seconds': timeout_seconds,
        'use_env_proxy': use_env_proxy,
    }


def build_requests_session(env_path: pathlib.Path = ENV_PATH) -> tuple[requests.Session, dict]:
    runtime = build_rocketreach_runtime(env_path)
    session = requests.Session()
    session.trust_env = runtime['use_env_proxy']
    return session, runtime


def normalize_linkedin_url(value: str) -> str:
    parsed = urlparse((value or '').strip())
    if not parsed.scheme or 'linkedin.com' not in parsed.netloc:
        return ''
    return f'{parsed.scheme}://{parsed.netloc}{parsed.path}'.rstrip('/')


def is_probable_linkedin_profile_url(value: str) -> bool:
    normalized = normalize_linkedin_url(value)
    if not normalized:
        return False
    path = urlparse(normalized).path.lower()
    return path.startswith('/in/') or path.startswith('/pub/')


def normalize_match_text(value: str) -> str:
    if not isinstance(value, str):
        return ''
    lowered = value.lower().replace('&', ' and ')
    return re.sub(r'[^a-z0-9]+', '', lowered)


def normalize_tokens(value: str) -> list[str]:
    if not isinstance(value, str):
        return []
    return [token for token in re.split(r'[^a-z0-9]+', value.lower()) if token]


def normalize_header_name(value) -> str:
    if not isinstance(value, str):
        return ''
    return ' '.join(value.strip().lower().split())


def row_value_by_aliases(row, aliases):
    if not isinstance(row, dict):
        return ''

    normalized_aliases = {normalize_header_name(alias) for alias in aliases}

    for key, value in row.items():
        if normalize_header_name(key) not in normalized_aliases:
            continue
        if value is None:
            continue
        if isinstance(value, str):
            return value.strip()
        return str(value).strip()

    return ''


def row_has_any_alias(row, aliases) -> bool:
    if not isinstance(row, dict):
        return False
    normalized_aliases = {normalize_header_name(alias) for alias in aliases}
    return any(normalize_header_name(key) in normalized_aliases for key in row.keys())


def hr_link_from_row(row) -> str:
    aliased_value = row_value_by_aliases(row, HR_LINK_ALIASES)
    if aliased_value or row_has_any_alias(row, HR_LINK_ALIASES):
        return aliased_value

    if not isinstance(row, dict):
        return ''

    for value in row.values():
        if not isinstance(value, str):
            continue
        candidate = value.strip()
        if 'linkedin.com/' in candidate.lower():
            return candidate
    return ''


def hr_name_from_row(row) -> str:
    return row_value_by_aliases(row, HR_NAME_ALIASES)


def hr_position_from_row(row) -> str:
    return row_value_by_aliases(row, HR_POSITION_ALIASES)


def job_position_from_row(row) -> str:
    return row_value_by_aliases(row, JOB_POSITION_ALIASES)


def company_name_from_row(row) -> str:
    return row_value_by_aliases(row, COMPANY_NAME_ALIASES)


def job_link_from_row(row) -> str:
    return row_value_by_aliases(row, JOB_LINK_ALIASES)


def date_from_row(row) -> str:
    return row_value_by_aliases(row, DATE_ALIASES)


def submitted_from_row(row) -> str:
    return row_value_by_aliases(row, SUBMITTED_ALIASES)


def make_search_payload(linkedin_url: str) -> dict:
    return {
        'start': 1,
        'page_size': 5,
        'query': {
            'link': [linkedin_url]
        },
        'order_by': 'relevance'
    }


def make_name_company_search_payload(name: str, company_name: str) -> dict:
    return {
        'start': 1,
        'page_size': 10,
        'query': {
            'name': [name],
            'current_employer': [company_name],
        },
        'order_by': 'relevance',
    }


def looks_like_profile(data: dict) -> bool:
    if not isinstance(data, dict):
        return False
    if 'id' not in data:
        return False
    person_like_fields = (
        'name',
        'linkedin_url',
        'recommended_email',
        'recommended_professional_email',
        'current_work_email',
        'emails',
        'current_title',
    )
    return any(field in data for field in person_like_fields)


def extract_profiles(data: dict) -> list[dict]:
    if not isinstance(data, dict):
        return []
    profiles: list[dict] = []
    for key in ('profile', 'profiles', 'people', 'results'):
        value = data.get(key)
        if isinstance(value, dict):
            profiles.append(value)
            continue
        if isinstance(value, list):
            for item in value:
                if isinstance(item, dict):
                    profiles.append(item)
    if looks_like_profile(data):
        profiles.append(data)

    deduped: list[dict] = []
    seen: set[str] = set()
    for profile in profiles:
        key = '|'.join((
            normalize_match_text(profile.get('id', '')),
            normalize_match_text(profile.get('name', '')),
            normalize_match_text(profile.get('linkedin_url') or (profile.get('links') or {}).get('linkedin', '')),
        ))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(profile)
    return deduped


def pick_profile(data: dict):
    profiles = extract_profiles(data)
    return profiles[0] if profiles else None


def extract_message(data):
    if not isinstance(data, dict):
        return None
    return data.get('detail') or data.get('message') or data.get('error') or data.get('code')


def is_authentication_failure(status_code: int | None, data) -> bool:
    if status_code not in {401, 403}:
        return False
    message = (extract_message(data) or '').lower()
    if status_code == 403 and message_has_quota_issue(message):
        return False
    if not message:
        return True
    return 'authentication' in message or 'api key' in message or 'invalid key' in message


def raise_for_authentication_failure(status_code: int | None, data) -> None:
    if not is_authentication_failure(status_code, data):
        return
    message = extract_message(data) or 'RocketReach authentication failed.'
    raise RuntimeError(f'RocketReach authentication failed: {message}')


def is_full_email(value):
    if not isinstance(value, str):
        return False
    trimmed = value.strip()
    if not trimmed or '@' not in trimmed:
        return False
    local_part, domain_part = trimmed.split('@', 1)
    if not local_part or not domain_part or '.' not in domain_part:
        return False
    if '*' in local_part:
        return False
    return True


def is_email_preview(value):
    if not isinstance(value, str):
        return False
    trimmed = value.strip()
    if not trimmed or is_full_email(trimmed):
        return False
    return trimmed.startswith('@') and '.' in trimmed[1:]


def collect_emails(profile, limit=2):
    if not isinstance(profile, dict):
        return []

    ordered = []
    seen = set()

    def add_candidate(value):
        if not is_full_email(value):
            return
        normalized = value.strip().lower()
        if normalized in seen:
            return
        seen.add(normalized)
        ordered.append(value.strip())

    add_candidate(profile.get('recommended_email'))
    add_candidate(profile.get('recommended_professional_email'))
    add_candidate(profile.get('current_work_email'))

    for expected_type in ('professional', 'personal'):
        for item in profile.get('emails', []) if isinstance(profile.get('emails'), list) else []:
            if not isinstance(item, dict):
                continue
            if (item.get('type') or '').lower() != expected_type:
                continue
            add_candidate(item.get('email'))

    for item in profile.get('emails', []) if isinstance(profile.get('emails'), list) else []:
        if not isinstance(item, dict):
            continue
        add_candidate(item.get('email'))

    if limit is None:
        return ordered
    return ordered[:limit]


def collect_email_previews(profile, limit=1):
    if not isinstance(profile, dict):
        return []

    ordered = []
    seen = set()

    def add_candidate(value):
        if not is_email_preview(value):
            return
        normalized = value.strip().lower()
        if normalized in seen:
            return
        seen.add(normalized)
        ordered.append(value.strip())

    for key in (
        'recommended_email',
        'recommended_professional_email',
        'current_work_email',
        'recommended_personal_email',
        'email',
        'email_domain',
        'current_work_email_domain',
        'recommended_email_domain',
    ):
        add_candidate(profile.get(key))

    for item in profile.get('emails', []) if isinstance(profile.get('emails'), list) else []:
        if isinstance(item, dict):
            add_candidate(item.get('email'))
            add_candidate(item.get('domain'))
        else:
            add_candidate(item)

    if limit is None:
        return ordered
    return ordered[:limit]


def primary_email(profile):
    emails = collect_emails(profile, limit=1)
    return emails[0] if emails else ''


def secondary_email(profile):
    emails = collect_emails(profile, limit=2)
    return emails[1] if len(emails) > 1 else ''


def looks_like_real_phone(value):
    if not isinstance(value, str):
        return False
    trimmed = value.strip()
    if not trimmed:
        return False
    if 'X' in trimmed.upper() or '*' in trimmed:
        return False
    digits = ''.join(ch for ch in trimmed if ch.isdigit())
    return len(digits) >= 6


def looks_like_phone_preview(value):
    if not isinstance(value, str):
        return False
    trimmed = value.strip()
    if not trimmed or looks_like_real_phone(trimmed):
        return False
    digits = ''.join(ch for ch in trimmed if ch.isdigit())
    return len(digits) >= 6 and ('X' in trimmed.upper() or '*' in trimmed)


def primary_phone(profile):
    if not isinstance(profile, dict):
        return ''

    for item in profile.get('phones', []) if isinstance(profile.get('phones'), list) else []:
        if not isinstance(item, dict):
            continue
        number = item.get('number')
        if looks_like_real_phone(number):
            return number.strip()
    return ''


def preview_phone(profile):
    if not isinstance(profile, dict):
        return ''

    for item in profile.get('phones', []) if isinstance(profile.get('phones'), list) else []:
        if not isinstance(item, dict):
            continue
        number = item.get('number')
        if looks_like_phone_preview(number):
            return number.strip()
    return ''


def has_usable_contact(profile):
    return bool(primary_email(profile) or primary_phone(profile))


def has_preview_contact(profile):
    return bool(collect_email_previews(profile, limit=1) or preview_phone(profile))


def profile_linkedin_url(profile) -> str:
    if not isinstance(profile, dict):
        return ''
    return fallback_str(profile.get('linkedin_url') or (profile.get('links') or {}).get('linkedin', ''))


def profile_company_candidates(profile) -> list[str]:
    if not isinstance(profile, dict):
        return []

    values: list[str] = []

    def add_candidate(value):
        if isinstance(value, str):
            cleaned = fallback_str(value)
            if cleaned:
                values.append(cleaned)
            return
        if isinstance(value, dict):
            for nested_key in ('name', 'company', 'company_name', 'organization', 'organization_name'):
                add_candidate(value.get(nested_key))
            return
        if isinstance(value, list):
            for item in value:
                add_candidate(item)

    for key in (
        'current_employer',
        'current_employers',
        'current_company',
        'current_work_company',
        'company',
        'company_name',
        'organization',
        'organization_name',
        'current_employment',
        'employment',
        'job',
    ):
        add_candidate(profile.get(key))

    deduped: list[str] = []
    seen: set[str] = set()
    for value in values:
        normalized = normalize_match_text(value)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        deduped.append(value)
    return deduped


def name_match_score(profile_name: str, expected_name: str) -> int:
    normalized_profile = normalize_match_text(profile_name)
    normalized_expected = normalize_match_text(expected_name)
    if not normalized_profile or not normalized_expected:
        return 0
    if normalized_profile == normalized_expected:
        return 100

    profile_tokens = set(normalize_tokens(profile_name))
    expected_tokens = set(normalize_tokens(expected_name))
    if not profile_tokens or not expected_tokens:
        return 0
    if profile_tokens == expected_tokens:
        return 95
    overlap = len(profile_tokens & expected_tokens)
    if overlap == 0:
        return 0
    if overlap == len(expected_tokens):
        return 75
    return 40


def company_match_score(candidate_company: str, expected_company: str) -> int:
    normalized_candidate = normalize_match_text(candidate_company)
    normalized_expected = normalize_match_text(expected_company)
    if not normalized_candidate or not normalized_expected:
        return 0
    if normalized_candidate == normalized_expected:
        return 100
    if normalized_candidate in normalized_expected or normalized_expected in normalized_candidate:
        return 70

    candidate_tokens = set(normalize_tokens(candidate_company))
    expected_tokens = set(normalize_tokens(expected_company))
    if not candidate_tokens or not expected_tokens:
        return 0
    overlap = len(candidate_tokens & expected_tokens)
    if overlap == 0:
        return 0
    if overlap == len(expected_tokens):
        return 60
    return 30


def title_match_score(candidate_title: str, expected_title: str) -> int:
    normalized_candidate = normalize_match_text(candidate_title)
    normalized_expected = normalize_match_text(expected_title)
    if not normalized_candidate or not normalized_expected:
        return 0
    if normalized_candidate == normalized_expected:
        return 100
    if normalized_candidate in normalized_expected or normalized_expected in normalized_candidate:
        return 70

    candidate_tokens = set(normalize_tokens(candidate_title))
    expected_tokens = set(normalize_tokens(expected_title))
    if not candidate_tokens or not expected_tokens:
        return 0
    overlap = len(candidate_tokens & expected_tokens)
    if overlap == 0:
        return 0
    if overlap == len(expected_tokens):
        return 60
    return 25


def contact_role_score(candidate_title: str) -> int:
    tokens = set(normalize_tokens(candidate_title))
    if not tokens:
        return 0
    recruiting_tokens = {
        'acquisition',
        'headhunter',
        'hiring',
        'hr',
        'human',
        'people',
        'recruiter',
        'recruiting',
        'staffing',
        'talent',
    }
    management_tokens = {
        'director',
        'head',
        'lead',
        'manager',
        'principal',
        'vp',
    }
    if tokens & recruiting_tokens:
        return 2
    if tokens & management_tokens:
        return 1
    return 0


def profile_identity_score(profile):
    if not isinstance(profile, dict):
        return -1

    score = 0
    if fallback_str(profile.get('name', '')):
        score += 1
    if fallback_str(profile.get('current_title', '') or profile.get('title', '')):
        score += 1
    linkedin_url = profile.get('linkedin_url') or (profile.get('links') or {}).get('linkedin', '')
    if fallback_str(linkedin_url):
        score += 1
    return score


def pick_best_profile(
    *profiles,
    expected_name: str = '',
    expected_company: str = '',
    expected_title: str = '',
    expected_linkedin: str = '',
    require_name_match: bool = False,
    require_company_match: bool = False,
):
    best_profile = None
    best_score = None

    for index, profile in enumerate(profiles):
        if not isinstance(profile, dict):
            continue

        exact_linkedin_match = 0
        if expected_linkedin:
            exact_linkedin_match = int(
                normalize_linkedin_url(profile_linkedin_url(profile)) == normalize_linkedin_url(expected_linkedin)
            )

        matched_name_score = name_match_score(profile.get('name', ''), expected_name) if expected_name else 0
        matched_company_score = 0
        for company_candidate in profile_company_candidates(profile):
            matched_company_score = max(matched_company_score, company_match_score(company_candidate, expected_company))
        candidate_title = fallback_str(profile.get('current_title', '') or profile.get('title', ''))
        matched_title_score = title_match_score(candidate_title, expected_title) if expected_title else 0

        if require_name_match and expected_name and matched_name_score == 0:
            continue
        if require_company_match and expected_company and matched_company_score == 0:
            continue

        score = (
            exact_linkedin_match,
            matched_company_score,
            matched_name_score,
            matched_title_score,
            contact_role_score(candidate_title),
            len(collect_emails(profile, limit=2)),
            1 if primary_phone(profile) else 0,
            1 if has_preview_contact(profile) else 0,
            profile_identity_score(profile),
            -index,
        )
        if best_score is None or score > best_score:
            best_profile = profile
            best_score = score

    return best_profile


def fallback_str(value):
    if not isinstance(value, str):
        return ''
    cleaned = value.strip()
    if not cleaned or cleaned.lower() == 'unknown':
        return ''
    return cleaned


def row_has_values(row):
    if not isinstance(row, dict):
        return False
    return any(str(value or '').strip() for value in row.values())


def message_has_quota_issue(message):
    normalized = (message or '').lower()
    quota_markers = (
        'account verification',
        'credit',
        'free credits',
        'insufficient credits',
        'lookup limit',
        'out of credits',
        'quota',
        'rate limit',
        'throttle',
        'upgrade',
        'verify account',
        'verify your account',
        'verify your email',
    )
    return any(marker in normalized for marker in quota_markers)


def status_from_result(row, body):
    hr_link = hr_link_from_row(row)
    profile = body.get('profile') if isinstance(body, dict) else None
    any_profile_found = bool(body.get('any_profile_found')) if isinstance(body, dict) else False
    lookup_message = (body.get('lookup_message') or '') if isinstance(body, dict) else ''
    search_message = (body.get('search_message') or '') if isinstance(body, dict) else ''
    input_profile_url_valid = bool(body.get('input_profile_url_valid', True)) if isinstance(body, dict) else True
    fallback_attempted = bool(body.get('name_company_fallback_attempted')) if isinstance(body, dict) else False
    missing_input_link = not hr_link or hr_link.lower() == 'unknown'
    invalid_input_link = bool(hr_link) and not normalize_linkedin_url(hr_link)

    if profile:
        if has_usable_contact(profile):
            return 'matched'
        if has_preview_contact(profile):
            return 'preview_match'
        if message_has_quota_issue(lookup_message) or message_has_quota_issue(search_message):
            return 'lookup_quota_reached'
        return 'profile_only'

    if message_has_quota_issue(lookup_message) or message_has_quota_issue(search_message):
        return 'lookup_quota_reached'
    if any_profile_found:
        return 'profile_only'
    if missing_input_link and not fallback_attempted:
        return 'missing_hr_link'
    if invalid_input_link and not fallback_attempted:
        return 'invalid_hr_link'
    if not input_profile_url_valid and not fallback_attempted:
        return 'invalid_hr_link'
    return 'no_match'


def clean_output_row(row, body):
    profile = body.get('profile') if isinstance(body, dict) else None
    input_name = fallback_str(hr_name_from_row(row))
    input_link = fallback_str(hr_link_from_row(row))
    normalized_input_link = normalize_linkedin_url(input_link)
    valid_input_profile_link = normalized_input_link if is_probable_linkedin_profile_url(input_link) else ''
    input_hr_position = fallback_str(hr_position_from_row(row))
    input_job_position = fallback_str(job_position_from_row(row))
    input_company_name = fallback_str(company_name_from_row(row))
    input_job_link = fallback_str(job_link_from_row(row))
    input_date = fallback_str(date_from_row(row))
    input_submitted = fallback_str(submitted_from_row(row))

    output = {
        'Date': input_date,
        'Company Name': input_company_name,
        'Position': input_job_position,
        'Job Link': input_job_link,
        'Submitted': input_submitted,
        'HR Name': input_name,
        'HR Position': input_hr_position,
        'HR Profile Link': valid_input_profile_link,
        'HR Email': '',
        'HR Secondary Email': '',
        'HR Email Preview': '',
        'HR Contact': '',
        'HR Contact Preview': '',
        'RocketReach Status': status_from_result(row, body),
    }

    if not profile:
        return output

    output['HR Name'] = input_name or fallback_str(profile.get('name', ''))
    output['HR Position'] = input_hr_position or fallback_str(profile.get('current_title', '') or profile.get('title', ''))
    output['HR Profile Link'] = valid_input_profile_link or fallback_str(
        profile.get('linkedin_url') or (profile.get('links') or {}).get('linkedin', '')
    )
    output['HR Email'] = primary_email(profile)
    output['HR Secondary Email'] = secondary_email(profile)
    output['HR Contact'] = primary_phone(profile)
    email_previews = collect_email_previews(profile, limit=1)
    output['HR Email Preview'] = email_previews[0] if email_previews else ''
    output['HR Contact Preview'] = preview_phone(profile)
    return output


def process_csv_bytes_without_api(file_bytes, status: str, status_message: str = ''):
    text = file_bytes.decode('utf-8-sig', errors='replace')
    reader = csv.DictReader(io.StringIO(text))
    if not reader.fieldnames:
        raise ValueError('CSV header row is missing or unreadable.')

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=OUTPUT_COLUMNS, extrasaction='ignore')
    writer.writeheader()

    total = 0
    for row in reader:
        if not row_has_values(row):
            continue
        total += 1
        clean_row = clean_output_row(row, {})
        clean_row['RocketReach Status'] = status
        writer.writerow(clean_row)

    stats = {
        'total': total,
        'matched': 0,
        'preview_match': 0,
        'failed': total,
        'skipped': 0,
        'no_match': 0,
        'missing_hr_link': 0,
        'invalid_hr_link': 0,
        'profile_only': 0,
        'lookup_quota_reached': 0,
        'authentication_failed': total if status == 'authentication_failed' else 0,
        'sendable_rows': 0,
    }
    if status_message:
        stats['output_note'] = status_message
    return output.getvalue(), stats


def process_csv_bytes(file_bytes, headers):
    text = file_bytes.decode('utf-8-sig', errors='replace')
    reader = csv.DictReader(io.StringIO(text))
    if not reader.fieldnames:
        raise ValueError('CSV header row is missing or unreadable.')

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=OUTPUT_COLUMNS, extrasaction='ignore')
    writer.writeheader()

    stats = {
        'total': 0,
        'matched': 0,
        'preview_match': 0,
        'failed': 0,
        'skipped': 0,
        'no_match': 0,
        'missing_hr_link': 0,
        'invalid_hr_link': 0,
        'profile_only': 0,
        'lookup_quota_reached': 0,
        'authentication_failed': 0,
        'sendable_rows': 0,
    }

    for row in reader:
        if not row_has_values(row):
            continue
        stats['total'] += 1
        hr_link = hr_link_from_row(row)
        result = lookup_then_search(hr_link, headers, row=row)
        body = dict(result['body'])
        clean_row = clean_output_row(row, body)
        writer.writerow(clean_row)

        status = clean_row['RocketReach Status']
        if clean_row['HR Email'] or clean_row['HR Secondary Email']:
            stats['sendable_rows'] += 1

        if status == 'matched':
            stats['matched'] += 1
        elif status == 'preview_match':
            stats['preview_match'] += 1
        elif status == 'missing_hr_link':
            stats['skipped'] += 1
            stats['missing_hr_link'] += 1
        elif status == 'invalid_hr_link':
            stats['skipped'] += 1
            stats['invalid_hr_link'] += 1
        elif status == 'no_match':
            stats['no_match'] += 1
        elif status == 'profile_only':
            stats['failed'] += 1
            stats['profile_only'] += 1
        elif status == 'lookup_quota_reached':
            stats['failed'] += 1
            stats['lookup_quota_reached'] += 1
        else:
            stats['failed'] += 1

    return output.getvalue(), stats


def post_search(payload: dict, headers: dict, session: requests.Session | None = None, runtime: dict | None = None):
    runtime = runtime or build_rocketreach_runtime()
    session = session or build_requests_session()[0]
    response = session.post(runtime['search_url'], headers=headers, json=payload, timeout=runtime['timeout_seconds'])
    try:
        raw = response.json()
    except ValueError:
        raw = {'message': response.text}
    raise_for_authentication_failure(response.status_code, raw)
    return response, raw


def _write_text_to_candidate(path: pathlib.Path, csv_text: str) -> pathlib.Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(csv_text, encoding='utf-8-sig')
    return path


def write_output_csv(output_path: str | pathlib.Path, csv_text: str) -> tuple[pathlib.Path, str | None]:
    requested_path = pathlib.Path(output_path).expanduser().resolve()
    requested_path.parent.mkdir(parents=True, exist_ok=True)

    with tempfile.NamedTemporaryFile(
        mode='w',
        encoding='utf-8-sig',
        delete=False,
        dir=requested_path.parent,
        prefix=f'{requested_path.stem}.tmp-',
        suffix=requested_path.suffix,
    ) as handle:
        handle.write(csv_text)
        temp_path = pathlib.Path(handle.name)

    try:
        os.replace(temp_path, requested_path)
        return requested_path, None
    except PermissionError:
        temp_path.unlink(missing_ok=True)

    fallback_latest = requested_path.with_name(f'{requested_path.stem}_latest{requested_path.suffix}')
    try:
        _write_text_to_candidate(fallback_latest, csv_text)
        return fallback_latest, (
            f"Main recruiter CSV '{requested_path.name}' was locked; wrote fallback file '{fallback_latest.name}' instead."
        )
    except PermissionError:
        pass

    timestamped = requested_path.with_name(
        f"{requested_path.stem}_{datetime.now().strftime('%Y%m%d_%H%M%S')}{requested_path.suffix}"
    )
    try:
        _write_text_to_candidate(timestamped, csv_text)
        return timestamped, (
            f"Main recruiter CSV '{requested_path.name}' and fallback file '{fallback_latest.name}' were locked; "
            f"wrote '{timestamped.name}' instead."
        )
    except PermissionError as error:
        raise PermissionError(
            f"Could not write recruiter CSV to '{requested_path}', '{fallback_latest}', or a timestamped fallback file."
        ) from error


def lookup_then_search(linkedin_url: str, headers: dict, row: dict | None = None):
    session, runtime = build_requests_session()
    input_name = hr_name_from_row(row or {})
    input_company = company_name_from_row(row or {})
    input_title = hr_position_from_row(row or {}) or job_position_from_row(row or {})
    input_profile_url_valid = is_probable_linkedin_profile_url(linkedin_url)
    linkedin_profile_url = normalize_linkedin_url(linkedin_url) if input_profile_url_valid else ''

    lookup_params = {
        'linkedin_url': linkedin_profile_url,
        'lookup_type': 'standard',
    }
    lookup_status = None
    lookup_message = None
    lookup_raw = None
    lookup_profile = None

    try:
        if linkedin_profile_url:
            lookup_response = session.get(
                runtime['lookup_url'],
                headers={'Api-Key': headers['Api-Key']},
                params=lookup_params,
                timeout=runtime['timeout_seconds'],
            )
            lookup_status = lookup_response.status_code
            try:
                lookup_raw = lookup_response.json()
            except ValueError:
                lookup_raw = {'message': lookup_response.text}
            raise_for_authentication_failure(lookup_status, lookup_raw)

            if lookup_response.ok:
                lookup_profile = pick_best_profile(
                    *extract_profiles(lookup_raw),
                    expected_name=input_name,
                    expected_company=input_company,
                    expected_title=input_title,
                    expected_linkedin=linkedin_profile_url,
                )
                if has_usable_contact(lookup_profile):
                    return {
                        'status_code': 200,
                        'body': {
                            'mode': 'lookup',
                            'lookup_status': lookup_status,
                            'lookup_message': None,
                            'search_message': None,
                            'profile': lookup_profile,
                            'any_profile_found': bool(lookup_profile),
                            'input_profile_url_valid': input_profile_url_valid,
                            'name_company_fallback_attempted': False,
                            'raw': {
                                'lookup': lookup_raw,
                                'search_fallback': None,
                                'name_company_search': None,
                            },
                        },
                    }

            lookup_message = extract_message(lookup_raw) or ('RocketReach lookup failed.' if not lookup_response.ok else None)
    except requests.RequestException as exc:
        lookup_status = 502
        lookup_message = f'RocketReach lookup request failed: {exc}'
        lookup_raw = {'message': str(exc)}

    search_raw = None
    search_profile = None
    search_message = None
    if linkedin_profile_url:
        try:
            search_response, search_raw = post_search(
                make_search_payload(linkedin_profile_url),
                headers,
                session=session,
                runtime=runtime,
            )
            if search_response.ok:
                search_profile = pick_best_profile(
                    *extract_profiles(search_raw),
                    expected_name=input_name,
                    expected_company=input_company,
                    expected_title=input_title,
                    expected_linkedin=linkedin_profile_url,
                )
                best_profile = pick_best_profile(
                    lookup_profile,
                    search_profile,
                    expected_name=input_name,
                    expected_company=input_company,
                    expected_title=input_title,
                    expected_linkedin=linkedin_profile_url,
                )
                if best_profile or lookup_profile or search_profile:
                    return {
                        'status_code': 200,
                        'body': {
                            'mode': 'search_fallback' if lookup_profile else 'search',
                            'lookup_status': lookup_status,
                            'lookup_message': lookup_message,
                            'search_message': None,
                            'profile': best_profile,
                            'lookup_profile': lookup_profile,
                            'search_profile': search_profile,
                            'any_profile_found': bool(lookup_profile or search_profile),
                            'input_profile_url_valid': input_profile_url_valid,
                            'name_company_fallback_attempted': False,
                            'raw': {
                                'lookup': lookup_raw,
                                'search_fallback': search_raw,
                                'name_company_search': None,
                            },
                        },
                    }

            search_message = extract_message(search_raw) or 'RocketReach search fallback failed.'
        except requests.RequestException as exc:
            search_message = f'RocketReach search fallback request failed: {exc}'
            search_raw = {'message': str(exc)}

    if input_name and input_company:
        try:
            fallback_response, fallback_raw = post_search(
                make_name_company_search_payload(input_name, input_company),
                headers,
                session=session,
                runtime=runtime,
            )
            fallback_profiles = extract_profiles(fallback_raw) if fallback_response.ok else []
            fallback_profile = pick_best_profile(
                *fallback_profiles,
                expected_name=input_name,
                expected_company=input_company,
                expected_title=input_title,
                require_name_match=True,
                require_company_match=True,
            )
            return {
                'status_code': 200,
                'body': {
                    'mode': 'name_company_search',
                    'lookup_status': lookup_status,
                    'lookup_message': lookup_message,
                    'search_message': search_message,
                    'profile': fallback_profile,
                    'lookup_profile': lookup_profile,
                    'search_profile': search_profile,
                    'any_profile_found': bool(lookup_profile or search_profile or fallback_profile),
                    'input_profile_url_valid': input_profile_url_valid,
                    'name_company_fallback_attempted': True,
                    'raw': {
                        'lookup': lookup_raw,
                        'search_fallback': search_raw,
                        'name_company_search': fallback_raw,
                    },
                },
            }
        except requests.RequestException as exc:
            search_message = f'RocketReach name/company fallback request failed: {exc}'

    best_profile = pick_best_profile(
        lookup_profile,
        search_profile,
        expected_name=input_name,
        expected_company=input_company,
        expected_title=input_title,
        expected_linkedin=linkedin_profile_url,
    )
    return {
        'status_code': 200,
        'body': {
            'mode': 'lookup_failed',
            'lookup_status': lookup_status,
            'lookup_message': lookup_message,
            'search_message': search_message,
            'message': search_message,
            'profile': best_profile,
            'lookup_profile': lookup_profile,
            'search_profile': search_profile,
            'any_profile_found': bool(lookup_profile or search_profile),
            'input_profile_url_valid': input_profile_url_valid,
            'name_company_fallback_attempted': bool(input_name and input_company),
            'raw': {
                'lookup': lookup_raw,
                'search_fallback': search_raw,
                'name_company_search': None,
            },
        },
    }


def bulk_enrich(input_csv_path: str | pathlib.Path, output_csv_path: str | pathlib.Path, env_path: pathlib.Path = ENV_PATH) -> dict:
    input_path = pathlib.Path(input_csv_path).expanduser().resolve()
    output_path = pathlib.Path(output_csv_path).expanduser().resolve()
    file_bytes = input_path.read_bytes()
    if not file_bytes:
        raise ValueError('Uploaded CSV file is empty.')

    headers = build_rocketreach_headers(env_path)
    try:
        csv_text, stats = process_csv_bytes(file_bytes, headers)
    except RuntimeError as error:
        message = str(error)
        if 'RocketReach authentication failed:' not in message:
            raise
        csv_text, stats = process_csv_bytes_without_api(
            file_bytes,
            'authentication_failed',
            status_message=message,
        )
    written_path, output_note = write_output_csv(output_path, csv_text)
    stats['recruiters_csv_path'] = str(written_path)
    if output_note and stats.get('output_note'):
        stats['output_note'] = f"{stats['output_note']} {output_note}"
    elif output_note:
        stats['output_note'] = output_note
    return stats
