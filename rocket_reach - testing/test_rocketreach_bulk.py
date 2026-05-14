from __future__ import annotations

import pathlib
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = pathlib.Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import rocketreach_bulk as rr


class FakeResponse:
    def __init__(self, status_code: int, payload: dict):
        self.status_code = status_code
        self._payload = payload
        self.ok = 200 <= status_code < 300
        self.text = str(payload)

    def json(self):
        return self._payload


def make_profile(
    *,
    name: str,
    company: str = '',
    linkedin_url: str = '',
    email: str = '',
    secondary_email: str = '',
    email_preview: str = '',
    phone_preview: str = '',
) -> dict:
    emails = []
    if email:
        emails.append({'type': 'professional', 'email': email})
    if secondary_email:
        emails.append({'type': 'personal', 'email': secondary_email})
    profile = {
        'id': name.lower().replace(' ', '-'),
        'name': name,
        'linkedin_url': linkedin_url,
        'current_employer': company,
        'current_title': 'Recruiter',
        'emails': emails,
    }
    if email:
        profile['recommended_professional_email'] = email
    if email_preview:
        profile['recommended_email'] = email_preview
    if phone_preview:
        profile['phones'] = [{'number': phone_preview}]
    return profile


class RocketReachBulkTests(unittest.TestCase):
    def setUp(self) -> None:
        self.headers = {'Api-Key': 'test-key', 'Content-Type': 'application/json'}

    def test_name_company_fallback_populates_matched_contact(self) -> None:
        row = {
            'Company Name': 'AI Mujtama Pharmacy',
            'HR Name': 'Mohamed Serour',
            'HR Profile Link': 'https://www.linkedin.com/in/mohamed-serour-567167285',
        }

        with patch('rocketreach_bulk.requests.get', return_value=FakeResponse(200, {'results': []})), patch(
            'rocketreach_bulk.requests.post',
            side_effect=[
                FakeResponse(200, {'results': []}),
                FakeResponse(
                    200,
                    {
                        'results': [
                            make_profile(
                                name='Mohamed Serour',
                                company='AI Mujtama Pharmacy',
                                linkedin_url='https://www.linkedin.com/in/mohamed-serour-567167285',
                                email='mohamed@aimujtama.com',
                            ),
                            make_profile(
                                name='Mohamed Serour',
                                company='Other Company',
                                linkedin_url='https://www.linkedin.com/in/mohamed-serour-other',
                                email='wrong@other.com',
                            ),
                        ]
                    },
                ),
            ],
        ):
            result = rr.lookup_then_search(row['HR Profile Link'], self.headers, row=row)

        clean_row = rr.clean_output_row(row, result['body'])
        self.assertEqual(clean_row['RocketReach Status'], 'matched')
        self.assertEqual(clean_row['HR Email'], 'mohamed@aimujtama.com')

    def test_name_company_fallback_marks_profile_only_without_email(self) -> None:
        row = {
            'Company Name': 'Quantum Australia',
            'HR Name': 'Muteb Hariri',
            'HR Profile Link': 'https://www.linkedin.com/in/muteb-hariri-509b14103',
        }

        with patch('rocketreach_bulk.requests.get', return_value=FakeResponse(200, {'results': []})), patch(
            'rocketreach_bulk.requests.post',
            side_effect=[
                FakeResponse(200, {'results': []}),
                FakeResponse(
                    200,
                    {
                        'results': [
                            make_profile(
                                name='Muteb Hariri',
                                company='Quantum Australia',
                                linkedin_url='https://www.linkedin.com/in/muteb-hariri-509b14103',
                            )
                        ]
                    },
                ),
            ],
        ):
            result = rr.lookup_then_search(row['HR Profile Link'], self.headers, row=row)

        clean_row = rr.clean_output_row(row, result['body'])
        self.assertEqual(clean_row['RocketReach Status'], 'profile_only')
        self.assertEqual(clean_row['HR Email'], '')

    def test_exact_linkedin_preview_hit_becomes_preview_match(self) -> None:
        row = {
            'Company Name': 'TheDriveGroup',
            'HR Name': 'Caoimhe Sheahan',
            'HR Profile Link': 'https://www.linkedin.com/in/caoimhesheahan',
        }

        with patch(
            'rocketreach_bulk.requests.get',
            return_value=FakeResponse(
                200,
                {
                    'profile': make_profile(
                        name='Caoimhe Sheahan',
                        company='TheDriveGroup',
                        linkedin_url='https://www.linkedin.com/in/caoimhesheahan',
                        email_preview='@thedrivegroup.com.au',
                        phone_preview='614-128-XXXX',
                    )
                },
            ),
        ), patch('rocketreach_bulk.requests.post', return_value=FakeResponse(200, {'results': []})):
            result = rr.lookup_then_search(row['HR Profile Link'], self.headers, row=row)

        clean_row = rr.clean_output_row(row, result['body'])
        self.assertEqual(clean_row['RocketReach Status'], 'preview_match')
        self.assertEqual(clean_row['HR Email'], '')
        self.assertEqual(clean_row['HR Email Preview'], '@thedrivegroup.com.au')
        self.assertEqual(clean_row['HR Contact Preview'], '614-128-XXXX')

    def test_name_company_fallback_preview_hit_becomes_preview_match(self) -> None:
        row = {
            'Company Name': 'TheDriveGroup',
            'HR Name': 'Caoimhe Sheahan',
            'HR Profile Link': 'https://www.linkedin.com/jobs/view/4396049957',
        }

        with patch('rocketreach_bulk.requests.get') as get_request, patch(
            'rocketreach_bulk.requests.post',
            return_value=FakeResponse(
                200,
                {
                    'results': [
                        make_profile(
                            name='Caoimhe Sheahan',
                            company='TheDriveGroup',
                            linkedin_url='https://www.linkedin.com/in/caoimhesheahan',
                            email_preview='@thedrivegroup.com.au',
                        )
                    ]
                },
            ),
        ):
            result = rr.lookup_then_search(row['HR Profile Link'], self.headers, row=row)

        clean_row = rr.clean_output_row(row, result['body'])
        get_request.assert_not_called()
        self.assertEqual(clean_row['RocketReach Status'], 'preview_match')
        self.assertEqual(clean_row['HR Email Preview'], '@thedrivegroup.com.au')

    def test_job_link_without_fallback_match_stays_no_match_not_invalid(self) -> None:
        row = {
            'Company Name': 'Slick Solutions',
            'HR Name': 'Unknown Recruiter',
            'HR Profile Link': 'https://www.linkedin.com/jobs/view/4396049957',
        }

        with patch('rocketreach_bulk.requests.get') as get_request, patch(
            'rocketreach_bulk.requests.post',
            return_value=FakeResponse(200, {'results': []}),
        ):
            result = rr.lookup_then_search(row['HR Profile Link'], self.headers, row=row)

        clean_row = rr.clean_output_row(row, result['body'])
        get_request.assert_not_called()
        self.assertEqual(clean_row['RocketReach Status'], 'no_match')

    def test_blank_hr_profile_link_does_not_fall_back_to_job_link(self) -> None:
        row = {
            'Company Name': 'Acme Corp',
            'Position': 'Software Engineer',
            'Job Link': 'https://www.linkedin.com/jobs/view/4399999999',
            'HR Name': '',
            'HR Profile Link': '',
        }

        with patch('rocketreach_bulk.requests.get') as get_request, patch('rocketreach_bulk.requests.post') as post_request:
            result = rr.lookup_then_search(row['HR Profile Link'], self.headers, row=row)

        clean_row = rr.clean_output_row(row, result['body'])
        get_request.assert_not_called()
        post_request.assert_not_called()
        self.assertEqual(clean_row['RocketReach Status'], 'missing_hr_link')
        self.assertEqual(clean_row['HR Profile Link'], '')

    def test_wrong_company_fallback_candidate_stays_no_match(self) -> None:
        row = {
            'Company Name': 'CloudMarc',
            'HR Name': 'Dilan Shrimpton',
            'HR Profile Link': 'https://www.linkedin.com/in/dilanshrimpton',
        }

        with patch('rocketreach_bulk.requests.get', return_value=FakeResponse(200, {'results': []})), patch(
            'rocketreach_bulk.requests.post',
            side_effect=[
                FakeResponse(200, {'results': []}),
                FakeResponse(
                    200,
                    {
                        'results': [
                            make_profile(
                                name='Dilan Shrimpton',
                                company='Totally Different Company',
                                linkedin_url='https://www.linkedin.com/in/dilan-wrong',
                                email='wrong@example.com',
                            )
                        ]
                    },
                ),
            ],
        ):
            result = rr.lookup_then_search(row['HR Profile Link'], self.headers, row=row)

        clean_row = rr.clean_output_row(row, result['body'])
        self.assertEqual(clean_row['RocketReach Status'], 'no_match')

    def test_credit_verification_message_becomes_lookup_quota_reached(self) -> None:
        row = {
            'Company Name': 'Durlston Partners',
            'HR Name': 'Jared Wolfaardt',
            'HR Profile Link': 'https://www.linkedin.com/in/jared-wolfaardt',
        }

        message = 'Please verify your account in order to receive the rest of your free credits here.'
        body = {
            'lookup_message': message,
            'search_message': '',
            'profile': None,
            'any_profile_found': False,
            'input_profile_url_valid': True,
            'name_company_fallback_attempted': False,
        }

        self.assertFalse(rr.is_authentication_failure(403, {'message': message}))
        clean_row = rr.clean_output_row(row, body)
        self.assertEqual(clean_row['RocketReach Status'], 'lookup_quota_reached')
        self.assertEqual(clean_row['HR Email'], '')

    def test_job_link_in_hr_profile_link_uses_name_company_fallback(self) -> None:
        row = {
            'Company Name': 'PFOnApp - UAN, PF Balance',
            'HR Name': 'Shaurya Mishra',
            'HR Profile Link': 'https://www.linkedin.com/jobs/view/4396011847',
        }

        with patch('rocketreach_bulk.requests.get') as get_request, patch(
            'rocketreach_bulk.requests.post',
            return_value=FakeResponse(
                200,
                {
                    'results': [
                        make_profile(
                            name='Shaurya Mishra',
                            company='PFOnApp - UAN, PF Balance',
                            linkedin_url='https://www.linkedin.com/in/shaurya-mishraa',
                            email='shaurya@pfonapp.com',
                        )
                    ]
                },
            ),
        ) as post_request:
            result = rr.lookup_then_search(row['HR Profile Link'], self.headers, row=row)

        clean_row = rr.clean_output_row(row, result['body'])
        get_request.assert_not_called()
        self.assertEqual(post_request.call_count, 1)
        self.assertEqual(clean_row['RocketReach Status'], 'matched')
        self.assertEqual(clean_row['HR Email'], 'shaurya@pfonapp.com')

    def test_invalid_job_link_is_not_copied_into_output_profile_link(self) -> None:
        row = {
            'Company Name': 'Slick Solutions',
            'HR Name': 'Unknown Recruiter',
            'HR Profile Link': 'https://www.linkedin.com/jobs/view/4396049957',
        }

        with patch('rocketreach_bulk.requests.get') as get_request, patch(
            'rocketreach_bulk.requests.post',
            return_value=FakeResponse(200, {'results': []}),
        ):
            result = rr.lookup_then_search(row['HR Profile Link'], self.headers, row=row)

        clean_row = rr.clean_output_row(row, result['body'])
        get_request.assert_not_called()
        self.assertEqual(clean_row['HR Profile Link'], '')

    def test_exact_linkedin_match_beats_nonmatching_company_profile(self) -> None:
        exact = make_profile(
            name='Mohamed Serour',
            company='AI Mujtama Pharmacy',
            linkedin_url='https://www.linkedin.com/in/mohamed-serour-567167285',
            email='right@example.com',
        )
        wrong = make_profile(
            name='Mohamed Serour',
            company='Other Company',
            linkedin_url='https://www.linkedin.com/in/mohamed-serour-other',
            email='wrong@example.com',
        )

        best = rr.pick_best_profile(
            wrong,
            exact,
            expected_name='Mohamed Serour',
            expected_company='AI Mujtama Pharmacy',
            expected_linkedin='https://www.linkedin.com/in/mohamed-serour-567167285',
        )

        self.assertEqual(best['recommended_professional_email'], 'right@example.com')

    def test_process_csv_bytes_end_to_end_populates_recruiter_csv(self) -> None:
        csv_bytes = (
            'Date,Company Name,Position,Job Link,Submitted,HR Name,HR Position,HR Profile Link\n'
            '03/04/2026,AI Mujtama Pharmacy,Cloud Engineer,https://linkedin.com/jobs/view/1,Applied,Mohamed Serour,3rd+,https://www.linkedin.com/in/mohamed-serour-567167285\n'
        ).encode('utf-8')

        with patch('rocketreach_bulk.requests.get', return_value=FakeResponse(200, {'results': []})), patch(
            'rocketreach_bulk.requests.post',
            side_effect=[
                FakeResponse(200, {'results': []}),
                FakeResponse(
                    200,
                    {
                        'results': [
                            make_profile(
                                name='Mohamed Serour',
                                company='AI Mujtama Pharmacy',
                                linkedin_url='https://www.linkedin.com/in/mohamed-serour-567167285',
                                email='mohamed@aimujtama.com',
                                secondary_email='m.serour@gmail.com',
                            )
                        ]
                    },
                ),
            ],
        ):
            csv_text, stats = rr.process_csv_bytes(csv_bytes, self.headers)

        self.assertIn('mohamed@aimujtama.com', csv_text)
        self.assertEqual(stats['matched'], 1)
        self.assertEqual(stats['sendable_rows'], 1)

    def test_process_csv_bytes_skips_blank_rows(self) -> None:
        csv_bytes = (
            'Date,Company Name,Position,Job Link,Submitted,HR Name,HR Position,HR Profile Link\n'
            ',,,,,,,\n'
            '03/04/2026,AI Mujtama Pharmacy,Cloud Engineer,https://linkedin.com/jobs/view/1,Applied,Mohamed Serour,3rd+,https://www.linkedin.com/in/mohamed-serour-567167285\n'
        ).encode('utf-8')

        profile = make_profile(
            name='Mohamed Serour',
            company='AI Mujtama Pharmacy',
            linkedin_url='https://www.linkedin.com/in/mohamed-serour-567167285',
            email='mohamed@aimujtama.com',
        )
        with patch(
            'rocketreach_bulk.lookup_then_search',
            return_value={'status_code': 200, 'body': {'profile': profile, 'any_profile_found': True}},
        ) as lookup:
            csv_text, stats = rr.process_csv_bytes(csv_bytes, self.headers)

        self.assertEqual(stats['total'], 1)
        self.assertEqual(stats['matched'], 1)
        self.assertEqual(stats['sendable_rows'], 1)
        self.assertEqual(lookup.call_count, 1)
        self.assertNotIn(',,,,,,,,,,,,,', csv_text)

    def test_process_csv_bytes_counts_preview_match_without_sendable_email(self) -> None:
        csv_bytes = (
            'Date,Company Name,Position,Job Link,Submitted,HR Name,HR Position,HR Profile Link\n'
            '03/04/2026,TheDriveGroup,Javascript Recruiter,https://linkedin.com/jobs/view/2,Applied,Caoimhe Sheahan,3rd+,https://www.linkedin.com/in/caoimhesheahan\n'
        ).encode('utf-8')

        with patch(
            'rocketreach_bulk.requests.get',
            return_value=FakeResponse(
                200,
                {
                    'profile': make_profile(
                        name='Caoimhe Sheahan',
                        company='TheDriveGroup',
                        linkedin_url='https://www.linkedin.com/in/caoimhesheahan',
                        email_preview='@thedrivegroup.com.au',
                        phone_preview='614-128-XXXX',
                    )
                },
            ),
        ), patch('rocketreach_bulk.requests.post', return_value=FakeResponse(200, {'results': []})):
            csv_text, stats = rr.process_csv_bytes(csv_bytes, self.headers)

        self.assertIn('@thedrivegroup.com.au', csv_text)
        self.assertEqual(stats['preview_match'], 1)
        self.assertEqual(stats['sendable_rows'], 0)

    def test_process_csv_bytes_blank_hr_link_with_name_company_can_still_match(self) -> None:
        csv_bytes = (
            'Date,Company Name,Position,Job Link,Submitted,HR Name,HR Position,HR Profile Link\n'
            '03/04/2026,PFOnApp - UAN, PF Balance,Recruiter,https://linkedin.com/jobs/view/3,Applied,Shaurya Mishra,Talent Acquisition,\n'
        ).encode('utf-8')

        with patch('rocketreach_bulk.requests.get') as get_request, patch(
            'rocketreach_bulk.requests.post',
            return_value=FakeResponse(
                200,
                {
                    'results': [
                        make_profile(
                            name='Shaurya Mishra',
                            company='PFOnApp - UAN, PF Balance',
                            linkedin_url='https://www.linkedin.com/in/shaurya-mishraa',
                            email='shaurya@pfonapp.com',
                        )
                    ]
                },
            ),
        ):
            csv_text, stats = rr.process_csv_bytes(csv_bytes, self.headers)

        get_request.assert_not_called()
        self.assertIn('shaurya@pfonapp.com', csv_text)
        self.assertEqual(stats['matched'], 1)
        self.assertEqual(stats['missing_hr_link'], 0)

    def test_write_output_csv_updates_default_path(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = pathlib.Path(temp_dir) / 'recruiters_enriched.csv'
            written_path, output_note = rr.write_output_csv(output_path, 'a,b\n1,2\n')

        self.assertEqual(written_path.name, 'recruiters_enriched.csv')
        self.assertIsNone(output_note)

    def test_write_output_csv_falls_back_to_latest_when_main_path_locked(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = pathlib.Path(temp_dir) / 'recruiters_enriched.csv'
            original_replace = rr.os.replace

            def fake_replace(src, dst):
                if pathlib.Path(dst) == output_path:
                    raise PermissionError('locked')
                return original_replace(src, dst)

            with patch('rocketreach_bulk.os.replace', side_effect=fake_replace):
                written_path, output_note = rr.write_output_csv(output_path, 'a,b\n1,2\n')

            self.assertEqual(written_path.name, 'recruiters_enriched_latest.csv')
            self.assertIn('locked', output_note or '')

    def test_write_output_csv_falls_back_to_timestamped_file_when_main_and_latest_locked(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = pathlib.Path(temp_dir) / 'recruiters_enriched.csv'
            original_write = rr._write_text_to_candidate

            def fake_write(path, csv_text):
                if path.name in {'recruiters_enriched_latest.csv'}:
                    raise PermissionError('latest locked')
                return original_write(path, csv_text)

            with patch('rocketreach_bulk.os.replace', side_effect=PermissionError('main locked')), patch(
                'rocketreach_bulk._write_text_to_candidate',
                side_effect=fake_write,
            ):
                written_path, output_note = rr.write_output_csv(output_path, 'a,b\n1,2\n')

            self.assertTrue(written_path.name.startswith('recruiters_enriched_'))
            self.assertTrue(written_path.exists())
            self.assertIn('locked', output_note or '')

    def test_write_output_csv_raises_when_all_targets_fail(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = pathlib.Path(temp_dir) / 'recruiters_enriched.csv'
            with patch('rocketreach_bulk.os.replace', side_effect=PermissionError('main locked')), patch(
                'rocketreach_bulk._write_text_to_candidate',
                side_effect=PermissionError('all locked'),
            ):
                with self.assertRaises(PermissionError):
                    rr.write_output_csv(output_path, 'a,b\n1,2\n')


if __name__ == '__main__':
    unittest.main()
