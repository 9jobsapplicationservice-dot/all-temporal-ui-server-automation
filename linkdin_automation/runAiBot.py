'''
Author:     Sai Vignesh Golla
LinkedIn:   https://www.linkedin.com/in/saivigneshgolla/

Copyright (C) 2024 Sai Vignesh Golla

License:    GNU Affero General Public License
            https://www.gnu.org/licenses/agpl-3.0.en.html
            
GitHub:     https://github.com/GodsScion/Auto_job_applier_linkedIn

Support me: https://github.com/sponsors/GodsScion

version:    26.01.20.5.08
'''


# Imports
import os
import csv
import re
import time
try:
    import pyautogui
except Exception:
    pyautogui = None

import threading

import pathlib
import importlib.util

# Set CSV field size limit to prevent field size errors
csv.field_size_limit(1000000)  # Set to 1MB instead of default 131KB
from urllib.parse import urlparse
from random import choice, shuffle, randint
from datetime import datetime

from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support.select import Select
from selenium.webdriver.remote.webelement import WebElement
from selenium.common.exceptions import NoSuchElementException, ElementClickInterceptedException, NoSuchWindowException, ElementNotInteractableException, StaleElementReferenceException, WebDriverException

from config.personals import *
from config.questions import *
from config.search import *
from config.secrets import use_AI, username, password, ai_provider, linkedin_auto_login, target_job_link
from config.settings import *

from modules.open_chrome import *
from modules.helpers import *
from modules.clickers_and_finders import *
from modules.validator import validate_config

if use_AI:
    from modules.ai.openaiConnections import ai_create_openai_client, ai_extract_skills, ai_answer_question, ai_close_openai_client
    from modules.ai.deepseekConnections import deepseek_create_client, deepseek_extract_skills, deepseek_answer_question
    from modules.ai.geminiConnections import gemini_create_client, gemini_extract_skills, gemini_answer_question

from typing import Literal


if pyautogui: pyautogui.FAILSAFE = False

# Dialog helpers come from modules.helpers and are pipeline-aware.
# if use_resume_generator:    from resume_generator import is_logged_in_GPT, login_GPT, open_resume_chat, create_custom_resume


#< Global Variables and logics

if run_in_background == True:
    pause_at_failed_question = False
    pause_before_submit = False
    run_non_stop = False

first_name = first_name.strip()
middle_name = middle_name.strip()
last_name = last_name.strip()
full_name = first_name + " " + middle_name + " " + last_name if middle_name else first_name + " " + last_name

useNewResume = True
randomly_answered_questions = set()

tabs_count = 1
easy_applied_count = 0
external_jobs_count = 0
failed_count = 0
skip_count = 0
dailyEasyApplyLimitReached = False
applied_csv_lock_warned = False
external_csv_lock_warned = False
rows_written_to_applied_csv = 0
rows_written_to_external_csv = 0
rows_missing_hr_profile = 0
logged_external_job_links = set()
pipeline_mode = is_pipeline_mode()

re_experience = re.compile(r'[(]?\s*(\d+)\s*[)]?\s*[-to]*\s*\d*[+]*\s*year[s]?', re.IGNORECASE)


def format_applied_date(value: datetime | None = None) -> str:
    return (value or datetime.now()).strftime('%d/%m/%Y')


def is_session_invalid_error(error: Exception) -> bool:
    message = str(error).lower()
    session_markers = (
        'invalid session id',
        'target window already closed',
        'chrome not reachable',
        'disconnected',
        'session deleted because of page crash',
        'web view not found',
        'no such window',
    )
    return isinstance(error, NoSuchWindowException) or any(marker in message for marker in session_markers)


def get_job_listing_ids() -> list[str]:
    job_ids = []
    for job in driver.find_elements(By.XPATH, "//li[@data-occludable-job-id]"):
        try:
            job_id = job.get_dom_attribute('data-occludable-job-id')
        except StaleElementReferenceException:
            continue
        if job_id:
            job_ids.append(job_id)
    return job_ids


def find_job_card_by_id(job_id: str) -> WebElement:
    return driver.find_element(By.XPATH, f"//li[@data-occludable-job-id='{job_id}']")


def should_run_single_pipeline_pass() -> bool:
    return pipeline_mode


def should_stop_after_configured_easy_apply_limit() -> bool:
    return pipeline_mode and pipeline_max_easy_apply > 0 and easy_applied_count >= pipeline_max_easy_apply


def build_stage_summary(total_runs: int) -> dict[str, int | bool]:
    return {
        "jobs_applied": easy_applied_count,
        "external_links_logged": external_jobs_count,
        "rows_written_to_applied_csv": rows_written_to_applied_csv,
        "rows_written_to_external_csv": rows_written_to_external_csv,
        "rows_missing_hr_profile": rows_missing_hr_profile,
        "failed_jobs": failed_count,
        "skipped_jobs": skip_count,
        "total_runs": total_runs,
        "pipeline_mode": pipeline_mode,
    }


def wait_for_transient_overlays_to_clear(timeout: float = 5.0) -> bool:
    '''
    Wait briefly for LinkedIn loaders and blocking overlays to disappear.
    '''
    overlay_xpaths = [
        "//*[contains(@class, 'jobs-loader') and not(contains(@class, 'hidden'))]",
        "//*[contains(@class, 'artdeco-loader')]",
        "//*[contains(@class, 'artdeco-modal-overlay')]",
        "//*[@aria-busy='true']",
    ]
    deadline = time.time() + timeout

    while time.time() < deadline:
        blocking_overlay_found = False
        for overlay_xpath in overlay_xpaths:
            try:
                for overlay in driver.find_elements(By.XPATH, overlay_xpath):
                    try:
                        if overlay.is_displayed():
                            blocking_overlay_found = True
                            break
                    except StaleElementReferenceException:
                        continue
            except Exception:
                continue
            if blocking_overlay_found:
                break

        if not blocking_overlay_found:
            return True

        time.sleep(0.2)

    return False


def get_active_easy_apply_modal(timeout: float = 3.0) -> WebElement | None:
    '''
    Return the visible Easy Apply modal when it is present.
    '''
    try:
        return WebDriverWait(driver, timeout).until(
            EC.visibility_of_element_located((By.CLASS_NAME, "jobs-easy-apply-modal"))
        )
    except Exception:
        return None


def find_easy_apply_button(button_texts: list[str], timeout: float = 1.5) -> WebElement | None:
    '''
    Find an enabled Easy Apply button by visible label inside the active modal.
    '''
    deadline = time.time() + timeout
    while time.time() < deadline:
        modal = get_active_easy_apply_modal(0.5)
        if not modal:
            time.sleep(0.15)
            continue

        for button_text in button_texts:
            button_xpath = (
                ".//button[not(@disabled) and "
                f"(normalize-space()='{button_text}' or .//span[normalize-space()='{button_text}'])]"
            )
            try:
                button = modal.find_element(By.XPATH, button_xpath)
                if button.is_displayed():
                    return button
            except Exception:
                continue

        time.sleep(0.15)

    return None


def click_easy_apply_button(button_texts: list[str], timeout: float = 2.0, scroll_top: bool = False) -> bool:
    '''
    Click an Easy Apply button using modal-scoped lookup with loader/intercept retries.
    '''
    deadline = time.time() + timeout
    last_error = None

    while time.time() < deadline:
        button = find_easy_apply_button(button_texts, timeout=0.6)
        if not button:
            time.sleep(0.15)
            continue

        try:
            wait_for_transient_overlays_to_clear(2.0)
            scroll_to_view(driver, button, scroll_top)
            button.click()
            buffer(click_gap)
            return True
        except ElementClickInterceptedException as error:
            last_error = error
            wait_for_transient_overlays_to_clear(2.5)
            time.sleep(0.25)
        except StaleElementReferenceException as error:
            last_error = error
            time.sleep(0.2)
        except Exception as error:
            last_error = error
            time.sleep(0.2)

    if last_error:
        print_lg(f"Click Failed! Didn't find/click Easy Apply button {button_texts}", last_error)
    else:
        print_lg(f"Click Failed! Didn't find Easy Apply button {button_texts}")
    return False


EASY_APPLY_BUTTON_XPATH = ".//button[contains(@class,'jobs-apply-button') and contains(@class, 'artdeco-button--3') and contains(@aria-label, 'Easy')]"


def launch_easy_apply(job_id: str) -> tuple[WebElement | None, str]:
    '''
    Capture the job page before opening the Easy Apply modal.
    '''
    before_apply_screenshot_name = capture_application_screenshot(job_id, "Before Apply")
    if not try_xp(driver, EASY_APPLY_BUTTON_XPATH):
        return None, before_apply_screenshot_name

    modal = get_active_easy_apply_modal()
    if not modal:
        raise Exception("Easy Apply modal did not open")

    return modal, before_apply_screenshot_name

desired_salary_lakhs = str(round(desired_salary / 100000, 2))
desired_salary_monthly = str(round(desired_salary/12, 2))
desired_salary = str(desired_salary)

current_ctc_lakhs = str(round(current_ctc / 100000, 2))
current_ctc_monthly = str(round(current_ctc/12, 2))
current_ctc = str(current_ctc)

notice_period_months = str(notice_period//30)
notice_period_weeks = str(notice_period//7)
notice_period = str(notice_period)

aiClient = None
##> ------ Dheeraj Deshwal : dheeraj9811 Email:dheeraj20194@iiitd.ac.in/dheerajdeshwal9811@gmail.com - Feature ------
about_company_for_ai = None # TODO extract about company for AI
##<

#>


#< Login Functions
def is_logged_in_LN() -> bool:
    '''
    Function to check if user is logged-in in LinkedIn
    * Returns: `True` if user is logged-in or `False` if not
    '''
    current_url = (driver.current_url or "").lower()
    if "linkedin.com/feed" in current_url: return True
    if any(marker in current_url for marker in ("/login", "/uas/login", "/checkpoint", "/challenge")): return False
    if try_linkText(driver, "Sign in"): return False
    if try_xp(driver, '//button[@type="submit" and contains(text(), "Sign in")]', False):  return False
    if try_linkText(driver, "Join now"): return False
    print_lg("Didn't find Sign in link, so assuming user is logged in!")
    return True


def find_first_visible_login_element(selectors: list[tuple[str, str]], timeout: float = 12.0) -> WebElement:
    last_error: Exception | None = None
    deadline = time.time() + timeout
    while time.time() < deadline:
        for by, selector in selectors:
            try:
                element = driver.find_element(by, selector)
                if element.is_displayed() and element.is_enabled():
                    return element
            except Exception as error:
                last_error = error
        sleep(0.5)
    raise last_error or TimeoutError("Timed out waiting for LinkedIn login element.")


def fill_login_input(element: WebElement, value: str) -> None:
    try:
        element.clear()
    except Exception:
        pass
    try:
        element.send_keys(Keys.CONTROL + "a")
        element.send_keys(value)
        return
    except Exception:
        driver.execute_script(
            """
            arguments[0].focus();
            arguments[0].value = arguments[1];
            arguments[0].dispatchEvent(new Event('input', { bubbles: true }));
            arguments[0].dispatchEvent(new Event('change', { bubbles: true }));
            """,
            element,
            value,
        )


def submit_linkedin_login_form() -> None:
    submit_button = find_first_visible_login_element(
        [
            (By.XPATH, '//button[@type="submit"]'),
            (By.XPATH, '//button[contains(translate(normalize-space(.), "ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz"), "sign in")]'),
            (By.CSS_SELECTOR, 'button[data-litms-control-urn="login-submit"]'),
        ],
        timeout=8.0,
    )
    submit_button.click()


def prompt_manual_linkedin_login(reason_message: str) -> bool:
    '''
    Ask the user to log in manually, then confirm the session before continuing.
    '''
    print_lg(reason_message)
    show_alert(
        "Please login to LinkedIn manually in the opened browser window.\n\n"
        "After the login is complete, click OK. The bot will then ask you to confirm the login before it starts applying.",
        "Manual LinkedIn Login",
        "OK",
    )
    return manual_login_retry(is_logged_in_LN, 2)


def is_recoverable_linkedin_login_error(message: str) -> bool:
    lowered = (message or "").strip().lower()
    markers = (
        "linkedin login was not confirmed",
        "complete manual login in chrome and keep the browser window open",
        "automatic linkedin login did not complete successfully",
        "linkedin login needs manual confirmation",
        "linkedin session needs manual confirmation",
        "session was blocked by linkedin",
        "captcha",
        "checkpoint",
        "2fa",
        "login page/form was unavailable",
    )
    return any(marker in lowered for marker in markers)


def should_fail_fast_linkedin_login() -> bool:
    '''
    In pipeline mode with auto-login enabled, fail fast instead of silently
    waiting for manual login when the login form/page is not usable.
    '''
    return pipeline_mode and linkedin_auto_login


def is_valid_linkedin_job_link(job_link: str) -> bool:
    parsed = urlparse((job_link or "").strip())
    if parsed.scheme not in {"http", "https"}:
        return False
    hostname = (parsed.netloc or "").lower()
    if hostname not in {"linkedin.com", "www.linkedin.com"}:
        return False
    normalized_path = (parsed.path or "").rstrip("/")
    return normalized_path.startswith("/jobs/view/")


def extract_job_id_from_link(job_link: str) -> str:
    match = re.search(r"/jobs/view/(\d+)", job_link)
    return match.group(1) if match else "unknown"


def extract_current_job_page_details(job_link: str) -> tuple[str, str, str, str, str]:
    job_id = extract_job_id_from_link(job_link)
    title = "Unknown"
    company = "Unknown"
    work_location = "Unknown"
    work_style = "Unknown"

    title_selectors = [
        '//h1[contains(@class, "job-details-jobs-unified-top-card__job-title")]',
        '//h1[contains(@class, "t-24")]',
        '//h1',
    ]
    for selector in title_selectors:
        element = try_xp(driver, selector, False)
        if element and element.text.strip():
            title = element.text.strip()
            break

    company_selectors = [
        '(//div[contains(@class, "job-details-jobs-unified-top-card__company-name")]//a)[1]',
        '(//div[contains(@class, "jobs-unified-top-card__company-name")]//a)[1]',
        '(//div[contains(@class, "job-details-jobs-unified-top-card__primary-description")]//a)[1]',
    ]
    for selector in company_selectors:
        element = try_xp(driver, selector, False)
        if element and element.text.strip():
            company = element.text.strip()
            break

    top_card = try_find_by_classes(driver, [
        "job-details-jobs-unified-top-card__primary-description-container",
        "job-details-jobs-unified-top-card__primary-description",
        "jobs-unified-top-card__primary-description",
        "jobs-details__main-content",
    ])
    top_card_lines = [line.strip() for line in top_card.text.splitlines() if line.strip()] if top_card else []
    if company == "Unknown" and len(top_card_lines) > 1:
        company = top_card_lines[1]
    if len(top_card_lines) > 2:
        work_location = top_card_lines[2]
    elif len(top_card_lines) > 1:
        work_location = top_card_lines[-1]
    if "(" in work_location and ")" in work_location:
        work_style = work_location[work_location.rfind("(") + 1:work_location.rfind(")")]
        work_location = work_location[:work_location.rfind("(")].strip()

    return job_id, title, company, work_location, work_style


def navigate_to_target_job(job_link: str) -> None:
    if not is_valid_linkedin_job_link(job_link):
        raise ValueError(f"Invalid LinkedIn job link: {job_link}")
    driver.get(job_link)
    wait.until(lambda current_driver: "linkedin.com/jobs/view/" in (current_driver.current_url or "").lower())
    wait_for_transient_overlays_to_clear(4.0)


def apply_to_target_job(job_link: str) -> None:
    global failed_count, skip_count, easy_applied_count, pause_before_submit, pause_at_failed_question, useNewResume

    clean_job_link = (job_link or "").strip()
    if not clean_job_link:
        raise ValueError("target_job_link is empty.")

    applied_jobs = get_applied_job_ids()
    if clean_job_link in applied_jobs:
        print_lg(f"Target job already exists in applied jobs history: {clean_job_link}")
        return

    navigate_to_target_job(clean_job_link)
    job_id, title, company, work_location, work_style = extract_current_job_page_details(clean_job_link)
    print_lg(f'Trying target job "{title} | {company}". Job ID: {job_id}')

    try:
        already_applied_marker = try_find_by_classes(driver, ["jobs-s-apply__application-link", "jobs-apply-button--top-card"])
    except Exception:
        already_applied_marker = None
    if already_applied_marker and not try_xp(driver, ".//button[contains(@class,'jobs-apply-button') and contains(@aria-label, 'Easy')]", False):
        print_lg(f'Target job "{title} | {company}" does not have Easy Apply. Skipping without crash.')
        skip_count += 1
        return

    application_submitted = False
    date_applied = "Pending"
    hr_link = ""
    hr_name = ""
    hr_position = ""
    connect_request = "In Development"
    date_listed = "Unknown"
    skills = "Needs an AI"
    resume = "Pending"
    reposted = False
    questions_list = None
    screenshot_name = "Not Available"

    try:
        hr_name, hr_link, hr_position = extract_hr_details()
        if not (hr_name or hr_link):
            print_lg(f'HR info was not given for "{title}" with Job ID: {job_id}!')
    except Exception as error:
        print_lg(f'Failed to extract HR info for "{title}" with Job ID: {job_id}!', error)

    try:
        time_posted_element = try_xp(driver, './/span[contains(normalize-space(), " ago")]', False)
        if time_posted_element:
            time_posted_text = time_posted_element.text
            if "Reposted" in time_posted_text:
                reposted = True
                time_posted_text = time_posted_text.replace("Reposted", "")
            date_listed = calculate_date_posted(time_posted_text.strip())
    except Exception as error:
        print_lg("Failed to calculate the date posted!", error)

    description, experience_required, skip, reason, message = get_job_description()
    if skip:
        print_lg(message)
        failed_job(job_id, clean_job_link, resume, date_listed, reason, message, "Skipped", screenshot_name)
        skip_count += 1
        return

    if use_AI and description != "Unknown":
        try:
            if ai_provider.lower() == "openai":
                skills = ai_extract_skills(aiClient, description)
            elif ai_provider.lower() == "deepseek":
                skills = deepseek_extract_skills(aiClient, description)
            elif ai_provider.lower() == "gemini":
                skills = gemini_extract_skills(aiClient, description)
            print_lg(f"Extracted skills using {ai_provider} AI")
        except Exception as error:
            print_lg("Failed to extract skills:", error)
            skills = "Error extracting skills"

    if not try_xp(driver, EASY_APPLY_BUTTON_XPATH, False):
        print_lg(f'Target job "{title} | {company}" does not expose an Easy Apply button on the page. Skipping without crash.')
        skip_count += 1
        return

    try:
        errored = ""
        resume = "Previous resume"
        questions_list = set()
        next_counter = 0
        uploaded = False

        modal, before_apply_screenshot_name = launch_easy_apply(job_id)
        if not modal:
            print_lg(f'Target job "{title} | {company}" does not expose an Easy Apply button on the page. Skipping.')
            skip_count += 1
            return
        
        print_lg(f"Easy Apply clicked for {title}. Starting application flow...")

        while True:
            next_counter += 1
            if next_counter >= 15:
                if pause_at_failed_question:
                    show_alert("Couldn't answer one or more questions.\nPlease click \"Continue\" once done.\nDO NOT CLICK Back, Next or Review button in LinkedIn.\n\n\n\n\nYou can turn off \"Pause at failed question\" setting in config.py", "Help Needed", "Continue")
                    next_counter = 1
                    continue
                if questions_list:
                    print_lg("Stuck for one or some of the following questions...", questions_list)
                screenshot_name = screenshot(driver, job_id, "Failed at questions")
                errored = "stuck"
                raise Exception("Seems like stuck in a continuous loop of next/review, probably because of new questions.")

            modal = get_active_easy_apply_modal()
            if not modal:
                break

            questions_list = answer_questions(modal, questions_list, work_location, job_description=description)
            if useNewResume and not uploaded:
                uploaded, resume = upload_resume(modal, default_resume_path)
            easy_apply_step_buffer()

            needs_manual_pause, pause_reason = detect_manual_form_needs(modal)
            if needs_manual_pause:
                pause_for_manual_form_completion(job_id, pause_reason)
                next_counter = 0
                easy_apply_step_buffer()
                continue

            if click_easy_apply_button(["Review"], timeout=1.2, scroll_top=True):
                easy_apply_step_buffer()
                break
            if click_easy_apply_button(["Next"], timeout=1.2, scroll_top=True):
                easy_apply_step_buffer()
                continue
            if find_easy_apply_button(["Submit application"], timeout=0.8) or application_sent_confirmation_present(modal):
                break

            screenshot_name = screenshot(driver, job_id, "Unable to advance Easy Apply")
            raise Exception("Could not find Next, Review, or Submit Application in Easy Apply modal")

        easy_apply_step_buffer()
        modal = get_active_easy_apply_modal(1.0)
        if modal and not application_sent_confirmation_present(modal):
            click_easy_apply_button(["Review"], timeout=1.0, scroll_top=True)
            modal = get_active_easy_apply_modal(1.0) or modal

        cur_pause_before_submit = pause_before_submit
        if errored != "stuck" and cur_pause_before_submit and modal and not application_sent_confirmation_present(modal):
            decision = show_confirm('1. Please verify your information.\n2. If you edited something, please return to this final screen.\n3. DO NOT CLICK "Submit Application".\n\n\n\n\nYou can turn off "Pause before submit" setting in config.py\nTo TEMPORARILY disable pausing, click "Disable Pause"', "Confirm your information", ["Disable Pause", "Discard Application", "Submit Application"])
            if decision == "Discard Application":
                raise Exception("Job application discarded by user!")
            pause_before_submit = False if "Disable Pause" == decision else True

        modal = get_active_easy_apply_modal(1.0)
        if modal:
            follow_company(modal)
        easy_apply_step_buffer()
        if click_easy_apply_button(["Submit application"], timeout=2.5, scroll_top=True):
            easy_apply_step_buffer()
            if confirmed_easy_apply_submission(3.5):
                date_applied = datetime.now()
                application_submitted = True
                print_lg(f"Application submitted successfully for {title} | {company}")
                capture_application_screenshot(job_id, "After Submitted")
                close_easy_apply_success_dialog()
        elif confirmed_easy_apply_submission(3.0):
            date_applied = datetime.now()
            application_submitted = True
            capture_application_screenshot(job_id, "After Submitted")
            close_easy_apply_success_dialog()
        elif errored != "stuck" and cur_pause_before_submit and "Yes" in show_confirm("You submitted the application, didn't you?", "Failed to find Submit Application!", ["Yes", "No"]):
            date_applied = datetime.now()
            application_submitted = True
            capture_application_screenshot(job_id, "After Submitted")
            close_easy_apply_success_dialog()
        else:
            print_lg("Since, Submit Application failed, discarding the job application...")
            if before_apply_screenshot_name:
                print_lg(f'Before Apply screenshot captured: {before_apply_screenshot_name}')
            raise Exception("Application was not confirmed as submitted")

        if not application_submitted:
            print_lg(f'Skipping applied-history save for "{title} | {company}" because the application was not confirmed submitted.')
            return

        saved_to_history = submitted_jobs(job_id, title, company, work_location, work_style, description, experience_required, skills, hr_name, hr_link, hr_position, resume, reposted, date_listed, date_applied, clean_job_link, "Easy Applied", questions_list, connect_request)
        if uploaded:
            useNewResume = False
        if saved_to_history:
            print_lg(f'Successfully saved target job "{title} | {company}". Job ID: {job_id}')
        else:
            print_lg(f'Target job "{title} | {company}" was submitted but could not be written to applied history.')
        easy_applied_count += 1
    except Exception as error:
        if application_submitted or confirmed_easy_apply_submission(1.5):
            if not isinstance(date_applied, datetime):
                date_applied = datetime.now()
            close_easy_apply_success_dialog()
            submitted_jobs(job_id, title, company, work_location, work_style, description, experience_required, skills, hr_name, hr_link, hr_position, resume, reposted, date_listed, date_applied, clean_job_link, "Easy Applied", questions_list, connect_request)
            easy_applied_count += 1
            return
        print_lg("Failed to Easy apply to target job!")
        critical_error_log("Somewhere in target Easy Apply process", error)
        failed_job(job_id, clean_job_link, resume, date_listed, "Problem in Easy Applying", error, "Easy Applied", screenshot_name)
        failed_count += 1
        discard_job()


def login_LN() -> bool:
    '''
    Function to login for LinkedIn
    * Tries to login using given `username` and `password` from `secrets.py`
    * If failed, tries to login using saved LinkedIn profile button if available
    * Falls back to manual login confirmation flow when automatic login is disabled or fails
    '''
    cleaned_username = username.strip()
    cleaned_password = password.strip()

    if not linkedin_auto_login:
        return prompt_manual_linkedin_login(
            "LinkedIn auto login is disabled in secrets.py. Please log in manually before the bot continues."
        )

    if (
        not cleaned_username
        or not cleaned_password
        or (cleaned_username == "username@example.com" and cleaned_password == "example_password")
    ):
        return prompt_manual_linkedin_login(
            "User did not configure username and password in secrets.py, hence automatic login is unavailable. Please log in manually."
        )

    login_urls = [
        "https://www.linkedin.com/login",
        "https://www.linkedin.com/uas/login",
    ]

    try:
        for login_url in login_urls:
            driver.get(login_url)
            buffer(2)
            if is_logged_in_LN():
                print_lg("Already logged in to LinkedIn.")
                return True

            try:
                username_field = find_first_visible_login_element(
                    [
                        (By.ID, "username"),
                        (By.ID, "session_key"),
                        (By.NAME, "session_key"),
                        (By.NAME, "username"),
                        (By.CSS_SELECTOR, 'input[type="email"]'),
                        (By.CSS_SELECTOR, 'input[autocomplete="username"]'),
                    ],
                    timeout=12.0,
                )
                password_field = find_first_visible_login_element(
                    [
                        (By.ID, "password"),
                        (By.ID, "session_password"),
                        (By.NAME, "session_password"),
                        (By.NAME, "password"),
                        (By.CSS_SELECTOR, 'input[type="password"]'),
                        (By.CSS_SELECTOR, 'input[autocomplete="current-password"]'),
                    ],
                    timeout=12.0,
                )
                fill_login_input(username_field, cleaned_username)
                fill_login_input(password_field, cleaned_password)
                submit_linkedin_login_form()
                break
            except Exception as form_error:
                print_lg(f"LinkedIn login form was not ready at {login_url}. Trying fallback if available.", form_error)
        else:
            raise RuntimeError("LinkedIn login form was not found on known login URLs.")
    except Exception:
        try:
            profile_button = find_by_class(driver, "profile__details")
            profile_button.click()
        except Exception:
            print_lg("Couldn't Login!")

    try:
        # Wait until successful redirect, indicating successful login
        WebDriverWait(driver, 30).until(lambda _: is_logged_in_LN())
        print_lg("Login successful!")
        return True
    except Exception as e:
        print_lg("Seems like login attempt failed! Possibly due to wrong credentials, captcha, 2FA, checkpoint, or an already-open LinkedIn session.")
        if should_fail_fast_linkedin_login():
            raise RuntimeError(
                "Automatic LinkedIn login did not complete successfully. "
                "The login page/form was unavailable or the session was blocked by LinkedIn. "
                "Complete manual login in Chrome and keep the browser window open."
            ) from e
        return prompt_manual_linkedin_login(
            "Automatic LinkedIn login did not complete successfully. Please finish the login manually, then confirm it in the popup."
        )
#>



def get_applied_job_ids() -> set[str]:
    '''
    Function to get a `set` of logged job links from existing applied jobs history csv file
    '''
    job_links: set[str] = set()
    try:
        with open(file_name, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            if reader.fieldnames and "Job Link" in reader.fieldnames:
                for row in reader:
                    job_link = (row.get("Job Link") or "").strip()
                    if job_link:
                        job_links.add(job_link)
    except FileNotFoundError:
        print_lg(f"The CSV file '{file_name}' does not exist.")
    return job_links



def set_search_location() -> None:
    '''
    Function to set search location
    '''
    if search_location.strip():
        try:
            print_lg(f'Setting search location as: "{search_location.strip()}"')
            search_location_ele = try_xp(driver, ".//input[@aria-label='City, state, or zip code'and not(@disabled)]", False) #  and not(@aria-hidden='true')]")
            text_input(actions, search_location_ele, search_location, "Search Location")
        except ElementNotInteractableException:
            try_xp(driver, ".//label[@class='jobs-search-box__input-icon jobs-search-box__keywords-label']")
            actions.send_keys(Keys.TAB, Keys.TAB).perform()
            actions.key_down(Keys.CONTROL).send_keys("a").key_up(Keys.CONTROL).perform()
            actions.send_keys(search_location.strip()).perform()
            sleep(2)
            actions.send_keys(Keys.ENTER).perform()
            try_xp(driver, ".//button[@aria-label='Cancel']")
        except Exception as e:
            try_xp(driver, ".//button[@aria-label='Cancel']")
            print_lg("Failed to update search location, continuing with default location!", e)


def apply_filters() -> None:
    '''
    Function to apply job search filters
    '''
    set_search_location()

    try:
        recommended_wait = 1 if click_gap < 1 else 0

        wait.until(EC.presence_of_element_located((By.XPATH, '//button[normalize-space()="All filters"]'))).click()
        buffer(recommended_wait)

        wait_span_click(driver, sort_by)
        wait_span_click(driver, date_posted)
        buffer(recommended_wait)

        multi_sel_noWait(driver, experience_level) 
        multi_sel_noWait(driver, companies, actions)
        if experience_level or companies: buffer(recommended_wait)

        multi_sel_noWait(driver, job_type)
        multi_sel_noWait(driver, on_site)
        if job_type or on_site: buffer(recommended_wait)

        if easy_apply_only: boolean_button_click(driver, actions, "Easy Apply")
        
        multi_sel_noWait(driver, location)
        multi_sel_noWait(driver, industry)
        if location or industry: buffer(recommended_wait)

        multi_sel_noWait(driver, job_function)
        multi_sel_noWait(driver, job_titles)
        if job_function or job_titles: buffer(recommended_wait)

        if under_10_applicants: boolean_button_click(driver, actions, "Under 10 applicants")
        if in_your_network: boolean_button_click(driver, actions, "In your network")
        if fair_chance_employer: boolean_button_click(driver, actions, "Fair Chance Employer")

        wait_span_click(driver, salary)
        buffer(recommended_wait)
        
        multi_sel_noWait(driver, benefits)
        multi_sel_noWait(driver, commitments)
        if benefits or commitments: buffer(recommended_wait)

        show_results_button: WebElement = driver.find_element(By.XPATH, '//button[contains(translate(@aria-label, "ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz"), "apply current filters to show")]')
        show_results_button.click()

        global pause_after_filters
        if pause_after_filters and "Turn off Pause after search" == show_confirm("These are your configured search results and filter. It is safe to change them while this dialog is open, any changes later could result in errors and skipping this search run.", "Please check your results", ["Turn off Pause after search", "Look's good, Continue"]):
            pause_after_filters = False

    except Exception as e:
        print_lg("Setting the preferences failed!")
        print_lg("Continuing with current search results after filter setup issue.", e)
        show_confirm(
            f"Faced error while applying filters. Please make sure correct filters are selected, click on show results and click on any button of this dialog. ERROR: {e}",
            "Filter setup issue",
            ["Doesn't look good, but Continue XD", "Look's good, Continue"],
        )



def get_page_info() -> tuple[WebElement | None, int | None]:
    '''
    Function to get pagination element and current page number
    '''
    try:
        pagination_element = try_find_by_classes(driver, ["jobs-search-pagination__pages", "artdeco-pagination", "artdeco-pagination__pages"])
        scroll_to_view(driver, pagination_element)
        current_page = int(pagination_element.find_element(By.XPATH, "//button[contains(@class, 'active')]").text)
    except Exception as e:
        print_lg("Failed to find Pagination element, hence couldn't scroll till end!")
        pagination_element = None
        current_page = None
        print_lg(e)
    return pagination_element, current_page



def get_job_main_details(job: WebElement, blacklisted_companies: set, rejected_jobs: set) -> tuple[str, str, str, str, str, bool]:
    '''
    # Function to get job main details.
    Returns a tuple of (job_id, title, company, work_location, work_style, skip)
    * job_id: Job ID
    * title: Job title
    * company: Company name
    * work_location: Work location of this job
    * work_style: Work style of this job (Remote, On-site, Hybrid)
    * skip: A boolean flag to skip this job
    '''
    skip = False
    job_id = job.get_dom_attribute('data-occludable-job-id') or 'unknown'
    title = 'Unknown'
    company = 'Unknown'
    work_location = 'Unknown'
    work_style = 'Unknown'

    try:
        job_details_button = job.find_element(By.TAG_NAME, 'a')  # job.find_element(By.CLASS_NAME, "job-card-list__title")  # Problem in India
        scroll_to_view(driver, job_details_button, True)
        title_text = (job_details_button.text or '').strip()
        title = title_text.split("\n", 1)[0] if title_text else 'Unknown'
    except Exception as error:
        print_lg(f'Skipping job card {job_id} because title link was not found.', error)
        buffer(click_gap)
        return (job_id, title, company, work_location, work_style, True)

    try:
        other_details = job.find_element(By.CLASS_NAME, 'artdeco-entity-lockup__subtitle').text
        index = other_details.find(' ?? ')
        if index != -1:
            company = other_details[:index]
            work_location = other_details[index+3:]
        else:
            company = other_details.strip() or 'Unknown'
            work_location = 'Unknown'
        if '(' in work_location and ')' in work_location:
            work_style = work_location[work_location.rfind('(')+1:work_location.rfind(')')]
            work_location = work_location[:work_location.rfind('(')].strip()
    except Exception as error:
        print_lg(f'Failed to fully parse company/location for "{title}" job card. Continuing with partial details.', error)

    if company in blacklisted_companies:
        print_lg(f'Skipping "{title} | {company}" job (Blacklisted Company). Job ID: {job_id}!')
        skip = True
    elif job_id in rejected_jobs:
        print_lg(f'Skipping previously rejected "{title} | {company}" job. Job ID: {job_id}!')
        skip = True
    try:
        if job.find_element(By.CLASS_NAME, "job-card-container__footer-job-state").text == "Applied":
            skip = True
            print_lg(f'Already applied to "{title} | {company}" job. Job ID: {job_id}!')
    except Exception:
        pass
    if not skip:
        last_click_error = None
        for attempt in range(3):
            try:
                wait_for_transient_overlays_to_clear(2.5)
                current_job = find_job_card_by_id(job_id)
                job_details_button = current_job.find_element(By.TAG_NAME, 'a')
                scroll_to_view(driver, job_details_button, True)
                job_details_button.click()
                last_click_error = None
                break
            except NoSuchElementException as error:
                last_click_error = error
                print_lg(f'Skipping "{title} | {company}" because the job card details button is missing. Retry {attempt + 1}/3', error)
                wait_for_transient_overlays_to_clear(1.0)
            except ElementClickInterceptedException as error:
                last_click_error = error
                print_lg(f'Job details click intercepted for "{title} | {company}". Retry {attempt + 1}/3', error)
                discard_job()
                wait_for_transient_overlays_to_clear(2.5)
            except StaleElementReferenceException as error:
                last_click_error = error
                print_lg(f'Recoverable stale element while clicking "{title} | {company}". Retry {attempt + 1}/3', error)
                wait_for_transient_overlays_to_clear(1.5)
        if last_click_error:
            print_lg(f'Skipping "{title} | {company}" after repeated job-card access failures. Job ID: {job_id}!')
            buffer(click_gap)
            return (job_id, title, company, work_location, work_style, True)
    buffer(click_gap)
    return (job_id, title, company, work_location, work_style, skip)


# Function to check for Blacklisted words in About Company
def check_blacklist(rejected_jobs: set, job_id: str, company: str, blacklisted_companies: set) -> tuple[set, set, WebElement] | ValueError:
    jobs_top_card = try_find_by_classes(driver, ["job-details-jobs-unified-top-card__primary-description-container","job-details-jobs-unified-top-card__primary-description","jobs-unified-top-card__primary-description","jobs-details__main-content"])
    about_company_org = find_by_class(driver, "jobs-company__box")
    scroll_to_view(driver, about_company_org)
    about_company_org = about_company_org.text
    about_company = about_company_org.lower()
    skip_checking = False
    for word in about_company_good_words:
        if word.lower() in about_company:
            print_lg(f'Found the word "{word}". So, skipped checking for blacklist words.')
            skip_checking = True
            break
    if not skip_checking:
        for word in about_company_bad_words: 
            if word.lower() in about_company: 
                rejected_jobs.add(job_id)
                blacklisted_companies.add(company)
                raise ValueError(f'\n"{about_company_org}"\n\nContains "{word}".')
    buffer(click_gap)
    scroll_to_view(driver, jobs_top_card)
    return rejected_jobs, blacklisted_companies, jobs_top_card



# Function to extract years of experience required from About Job
def extract_years_of_experience(text: str) -> int:
    # Extract all patterns like '10+ years', '5 years', '3-5 years', etc.
    matches = re.findall(re_experience, text)
    if len(matches) == 0: 
        print_lg(f'\n{text}\n\nCouldn\'t find experience requirement in About the Job!')
        return 0
    return max([int(match) for match in matches if int(match) <= 12])



def get_job_description(
) -> tuple[
    str | Literal['Unknown'],
    int | Literal['Unknown'],
    bool,
    str | None,
    str | None
    ]:
    '''
    # Job Description
    Function to extract job description from About the Job.
    ### Returns:
    - `jobDescription: str | 'Unknown'`
    - `experience_required: int | 'Unknown'`
    - `skip: bool`
    - `skipReason: str | None`
    - `skipMessage: str | None`
    '''
    try:
        ##> ------ Dheeraj Deshwal : dheeraj9811 Email:dheeraj20194@iiitd.ac.in/dheerajdeshwal9811@gmail.com - Feature ------
        jobDescription = "Unknown"
        ##<
        experience_required = "Unknown"
        found_masters = 0
        jobDescription = find_by_class(driver, "jobs-box__html-content").text
        jobDescriptionLow = jobDescription.lower()
        skip = False
        skipReason = None
        skipMessage = None
        for word in bad_words:
            if word.lower() in jobDescriptionLow:
                skipMessage = f'\n{jobDescription}\n\nContains bad word "{word}". Skipping this job!\n'
                skipReason = "Found a Bad Word in About Job"
                skip = True
                break
        if not skip and security_clearance == False and ('polygraph' in jobDescriptionLow or 'clearance' in jobDescriptionLow or 'secret' in jobDescriptionLow):
            skipMessage = f'\n{jobDescription}\n\nFound "Clearance" or "Polygraph". Skipping this job!\n'
            skipReason = "Asking for Security clearance"
            skip = True
        if not skip:
            if did_masters and 'master' in jobDescriptionLow:
                print_lg(f'Found the word "master" in \n{jobDescription}')
                found_masters = 2
            experience_required = extract_years_of_experience(jobDescription)
            if current_experience > -1 and experience_required > current_experience + found_masters:
                skipMessage = f'\n{jobDescription}\n\nExperience required {experience_required} > Current Experience {current_experience + found_masters}. Skipping this job!\n'
                skipReason = "Required experience is high"
                skip = True
    except Exception as e:
        if jobDescription == "Unknown":    print_lg("Unable to extract job description!")
        else:
            experience_required = "Error in extraction"
            print_lg("Unable to extract years of experience required!")
            # print_lg(e)

    return jobDescription, experience_required, skip, skipReason, skipMessage

        


# Function to upload resume
def upload_resume(modal: WebElement, resume: str) -> tuple[bool, str]:
    try:
        modal.find_element(By.NAME, "file").send_keys(os.path.abspath(resume))
        return True, os.path.basename(default_resume_path)
    except: return False, "Previous resume"

# Function to answer common questions for Easy Apply
def answer_common_questions(label: str, answer: str) -> str:
    if 'sponsorship' in label or 'visa' in label: answer = require_visa
    return answer


def easy_apply_step_buffer(multiplier: int = 1) -> None:
    pace = max(click_gap + manual_form_pacing, 0)
    for _ in range(max(multiplier, 1)):
        buffer(pace)


def get_manual_question_label(question: WebElement) -> str:
    selectors = [
        ".//label",
        ".//legend",
        ".//span[contains(@class, 'visually-hidden')]",
        ".//span[contains(@class, 'fb-dash-form-element__label')]",
    ]
    for selector in selectors:
        element = try_xp(question, selector, False)
        if element and element.text.strip():
            return element.text.strip()
    return "Unknown"


def question_needs_manual_help(question: WebElement) -> bool:
    validation_error = try_xp(question, ".//*[contains(@class, 'artdeco-inline-feedback') or contains(@class, 'fb-dash-form-element__error') or @role='alert'][normalize-space()]", False)
    if validation_error:
        return True

    question_text = question.text or ""
    question_html = (question.get_attribute("innerHTML") or "").lower()
    is_required = "*" in question_text or "required" in question_html
    if not is_required:
        return False

    radio_buttons = question.find_elements(By.XPATH, ".//input[@type='radio']")
    if radio_buttons and not any(radio.is_selected() for radio in radio_buttons):
        return True

    select_elements = question.find_elements(By.XPATH, ".//select")
    placeholder_values = {"", "select an option", "month", "year", "choose one", "choose an option"}
    for select_element in select_elements:
        try:
            selected_text = Select(select_element).first_selected_option.text.strip().lower()
        except Exception:
            selected_text = ""
        if selected_text in placeholder_values:
            return True

    text_inputs = question.find_elements(By.XPATH, ".//input[not(@type='hidden') and not(@type='radio') and not(@type='checkbox')]")
    for text_input in text_inputs:
        if not (text_input.get_attribute("value") or "").strip():
            return True

    text_areas = question.find_elements(By.XPATH, ".//textarea")
    for text_area in text_areas:
        if not (text_area.get_attribute("value") or "").strip():
            return True

    return False


def detect_manual_form_needs(modal: WebElement) -> tuple[bool, str]:
    manual_labels = []
    for question in modal.find_elements(By.XPATH, ".//div[@data-test-form-element]"):
        label = get_manual_question_label(question)
        label_lower = label.lower()
        question_text = (question.text or "").lower()
        if "email" in label_lower or "email" in question_text:
            for select_element in question.find_elements(By.XPATH, ".//select"):
                try:
                    selected_text = Select(select_element).first_selected_option.text.strip().lower()
                except Exception:
                    selected_text = ""
                if selected_text in {"", "select an option", "choose one", "choose an option"}:
                    return True, "Please select your email in the Easy Apply form"

        if not manual_pause_on_form:
            continue

        if question_needs_manual_help(question):
            if label not in manual_labels:
                manual_labels.append(label)

    if not manual_pause_on_form:
        return False, ""

    save_button = try_xp(modal, ".//button[normalize-space()='Save' or .//span[normalize-space()='Save']]", False)
    add_more_button = try_xp(modal, ".//button[contains(normalize-space(), 'Add more') or .//span[contains(normalize-space(), 'Add more')]]", False)
    if save_button or add_more_button:
        reason = "LinkedIn opened a custom form section that needs your input"
        if manual_labels:
            reason += ": " + ", ".join(manual_labels[:3])
        return True, reason

    if manual_labels:
        return True, "Required fields still need manual input: " + ", ".join(manual_labels[:3])

    return False, ""


def pause_for_manual_form_completion(job_id: str, reason: str) -> None:
    print_lg(f"Pausing for manual form completion. {reason}")
    screenshot(driver, job_id, "Manual form input needed")
    if pipeline_mode:
        timeout_seconds = int(os.environ.get("PIPELINE_MANUAL_FORM_TIMEOUT_SECONDS", "180") or "180")
        deadline = time.time() + max(timeout_seconds, 1)
        print_lg(
            f"Pipeline mode: {reason}. Please update the visible Easy Apply form. "
            f"The bot will continue automatically after the field is filled or after {timeout_seconds} seconds."
        )
        while time.time() < deadline:
            modal = get_active_easy_apply_modal(1.0)
            if not modal:
                return
            needs_manual_pause, _ = detect_manual_form_needs(modal)
            if not needs_manual_pause:
                print_lg("Manual Easy Apply field completed. Continuing automation.")
                return
            sleep(2)
        print_lg("Manual Easy Apply wait timed out. Continuing so the job can be skipped safely if still incomplete.")
        return

    show_alert(
        f"{reason}.\n\nPlease fill the visible LinkedIn form manually, then click Continue.\nThe bot will re-check the page and continue automatically.",
        "Manual input needed",
        "Continue",
    )


# Function to answer the questions for Easy Apply
def answer_questions(modal: WebElement, questions_list: set, work_location: str, job_description: str | None = None ) -> set:
    # Get all questions from the page
     
    all_questions = modal.find_elements(By.XPATH, ".//div[@data-test-form-element]")
    # all_questions = modal.find_elements(By.CLASS_NAME, "jobs-easy-apply-form-element")
    # all_list_questions = modal.find_elements(By.XPATH, ".//div[@data-test-text-entity-list-form-component]")
    # all_single_line_questions = modal.find_elements(By.XPATH, ".//div[@data-test-single-line-text-form-component]")
    # all_questions = all_questions + all_list_questions + all_single_line_questions

    for Question in all_questions:
        # Check if it's a select Question
        select = try_xp(Question, ".//select", False)
        if select:
            label_org = "Unknown"
            try:
                label = Question.find_element(By.TAG_NAME, "label")
                label_org = label.find_element(By.TAG_NAME, "span").text
            except: pass
            answer = 'Yes'
            label = label_org.lower()
            select = Select(select)
            selected_option = select.first_selected_option.text
            optionsText = []
            options = '"List of phone country codes"'
            if label != "phone country code":
                optionsText = [option.text for option in select.options]
                options = "".join([f' "{option}",' for option in optionsText])
            prev_answer = selected_option
            if overwrite_previous_answers or selected_option == "Select an option":
                ##> ------ WINDY_WINDWARD Email:karthik.sarode23@gmail.com - Added fuzzy logic to answer location based questions ------
                if 'email' in label or 'phone' in label: 
                    answer = prev_answer
                elif 'gender' in label or 'sex' in label: 
                    answer = gender
                elif 'disability' in label: 
                    answer = disability_status
                elif 'proficiency' in label: 
                    answer = 'Professional'
                # Add location handling
                elif any(loc_word in label for loc_word in ['location', 'city', 'state', 'country']):
                    if 'country' in label:
                        answer = country 
                    elif 'state' in label:
                        answer = state
                    elif 'city' in label:
                        answer = current_city if current_city else work_location
                    else:
                        answer = work_location
                else: 
                    answer = answer_common_questions(label,answer)
                try: 
                    select.select_by_visible_text(answer)
                except NoSuchElementException as e:
                    # Define similar phrases for common answers
                    possible_answer_phrases = []
                    if answer == 'Decline':
                        possible_answer_phrases = ["Decline", "not wish", "don't wish", "Prefer not", "not want"]
                    elif 'yes' in answer.lower():
                        possible_answer_phrases = ["Yes", "Agree", "I do", "I have"]
                    elif 'no' in answer.lower():
                        possible_answer_phrases = ["No", "Disagree", "I don't", "I do not"]
                    else:
                        # Try partial matching for any answer
                        possible_answer_phrases = [answer]
                        # Add lowercase and uppercase variants
                        possible_answer_phrases.append(answer.lower())
                        possible_answer_phrases.append(answer.upper())
                        # Try without special characters
                        possible_answer_phrases.append(''.join(c for c in answer if c.isalnum()))
                    ##<
                    foundOption = False
                    for phrase in possible_answer_phrases:
                        for option in optionsText:
                            # Check if phrase is in option or option is in phrase (bidirectional matching)
                            if phrase.lower() in option.lower() or option.lower() in phrase.lower():
                                select.select_by_visible_text(option)
                                answer = option
                                foundOption = True
                                break
                    if not foundOption:
                        #TODO: Use AI to answer the question need to be implemented logic to extract the options for the question
                        print_lg(f'Failed to find an option with text "{answer}" for question labelled "{label_org}", answering randomly!')
                        select.select_by_index(randint(1, len(select.options)-1))
                        answer = select.first_selected_option.text
                        randomly_answered_questions.add((f'{label_org} [ {options} ]',"select"))
            questions_list.add((f'{label_org} [ {options} ]', answer, "select", prev_answer))
            continue
        
        # Check if it's a radio Question
        radio = try_xp(Question, './/fieldset[@data-test-form-builder-radio-button-form-component="true"]', False)
        if radio:
            prev_answer = None
            label = try_xp(radio, './/span[@data-test-form-builder-radio-button-form-component__title]', False)
            try: label = find_by_class(label, "visually-hidden", 2.0)
            except: pass
            label_org = label.text if label else "Unknown"
            answer = 'Yes'
            label = label_org.lower()

            label_org += ' [ '
            options = radio.find_elements(By.TAG_NAME, 'input')
            options_labels = []
            
            for option in options:
                id = option.get_attribute("id")
                option_label = try_xp(radio, f'.//label[@for="{id}"]', False)
                options_labels.append( f'"{option_label.text if option_label else "Unknown"}"<{option.get_attribute("value")}>' ) # Saving option as "label <value>"
                if option.is_selected(): prev_answer = options_labels[-1]
                label_org += f' {options_labels[-1]},'

            if overwrite_previous_answers or prev_answer is None:
                if 'citizenship' in label or 'employment eligibility' in label: answer = us_citizenship
                elif 'veteran' in label or 'protected' in label: answer = veteran_status
                elif 'disability' in label or 'handicapped' in label: 
                    answer = disability_status
                else: answer = answer_common_questions(label,answer)
                foundOption = try_xp(radio, f".//label[normalize-space()='{answer}']", False)
                if foundOption: 
                    actions.move_to_element(foundOption).click().perform()
                else:    
                    possible_answer_phrases = ["Decline", "not wish", "don't wish", "Prefer not", "not want"] if answer == 'Decline' else [answer]
                    ele = options[0]
                    answer = options_labels[0]
                    for phrase in possible_answer_phrases:
                        for i, option_label in enumerate(options_labels):
                            if phrase in option_label:
                                foundOption = options[i]
                                ele = foundOption
                                answer = f'Decline ({option_label})' if len(possible_answer_phrases) > 1 else option_label
                                break
                        if foundOption: break
                    # if answer == 'Decline':
                    #     answer = options_labels[0]
                    #     for phrase in ["Prefer not", "not want", "not wish"]:
                    #         foundOption = try_xp(radio, f".//label[normalize-space()='{phrase}']", False)
                    #         if foundOption:
                    #             answer = f'Decline ({phrase})'
                    #             ele = foundOption
                    #             break
                    actions.move_to_element(ele).click().perform()
                    if not foundOption: randomly_answered_questions.add((f'{label_org} ]',"radio"))
            else: answer = prev_answer
            questions_list.add((label_org+" ]", answer, "radio", prev_answer))
            continue
        
        # Check if it's a text question
        text = try_xp(Question, ".//input[@type='text']", False)
        if text: 
            do_actions = False
            label = try_xp(Question, ".//label[@for]", False)
            try: label = label.find_element(By.CLASS_NAME,'visually-hidden')
            except: pass
            label_org = label.text if label else "Unknown"
            answer = "" # years_of_experience
            label = label_org.lower()

            prev_answer = text.get_attribute("value")
            if not prev_answer or overwrite_previous_answers:
                if 'experience' in label or 'years' in label: answer = years_of_experience
                elif 'phone' in label or 'mobile' in label: answer = phone_number
                elif 'street' in label: answer = street
                elif 'city' in label or 'location' in label or 'address' in label:
                    answer = current_city if current_city else work_location
                    do_actions = True
                elif 'signature' in label: answer = full_name # 'signature' in label or 'legal name' in label or 'your name' in label or 'full name' in label: answer = full_name     # What if question is 'name of the city or university you attend, name of referral etc?'
                elif 'name' in label:
                    if 'full' in label: answer = full_name
                    elif 'first' in label and 'last' not in label: answer = first_name
                    elif 'middle' in label and 'last' not in label: answer = middle_name
                    elif 'last' in label and 'first' not in label: answer = last_name
                    elif 'employer' in label: answer = recent_employer
                    else: answer = full_name
                elif 'notice' in label:
                    if 'month' in label:
                        answer = notice_period_months
                    elif 'week' in label:
                        answer = notice_period_weeks
                    else: answer = notice_period
                elif 'salary' in label or 'compensation' in label or 'ctc' in label or 'pay' in label: 
                    if 'current' in label or 'present' in label:
                        if 'month' in label:
                            answer = current_ctc_monthly
                        elif 'lakh' in label:
                            answer = current_ctc_lakhs
                        else:
                            answer = current_ctc
                    else:
                        if 'month' in label:
                            answer = desired_salary_monthly
                        elif 'lakh' in label:
                            answer = desired_salary_lakhs
                        else:
                            answer = desired_salary
                elif 'linkedin' in label: answer = linkedIn
                elif 'website' in label or 'blog' in label or 'portfolio' in label or 'link' in label: answer = website
                elif 'scale of 1-10' in label: answer = confidence_level
                elif 'headline' in label: answer = linkedin_headline
                elif ('hear' in label or 'come across' in label) and 'this' in label and ('job' in label or 'position' in label): answer = "https://github.com/GodsScion/Auto_job_applier_linkedIn"
                elif 'state' in label or 'province' in label: answer = state
                elif 'zip' in label or 'postal' in label or 'code' in label: answer = zipcode
                elif 'country' in label: answer = country
                else: answer = answer_common_questions(label,answer)
                ##> ------ Yang Li : MARKYangL - Feature ------
                if answer == "":
                    if use_AI and aiClient:
                        try:
                            if ai_provider.lower() == "openai":
                                answer = ai_answer_question(aiClient, label_org, question_type="text", job_description=job_description, user_information_all=user_information_all)
                            elif ai_provider.lower() == "deepseek":
                                answer = deepseek_answer_question(aiClient, label_org, options=None, question_type="text", job_description=job_description, about_company=None, user_information_all=user_information_all)
                            elif ai_provider.lower() == "gemini":
                                answer = gemini_answer_question(aiClient, label_org, options=None, question_type="text", job_description=job_description, about_company=None, user_information_all=user_information_all)
                            else:
                                randomly_answered_questions.add((label_org, "text"))
                                answer = years_of_experience
                            if answer and isinstance(answer, str) and len(answer) > 0:
                                print_lg(f'AI Answered received for question "{label_org}" \nhere is answer: "{answer}"')
                            else:
                                randomly_answered_questions.add((label_org, "text"))
                                answer = years_of_experience
                        except Exception as e:
                            print_lg("Failed to get AI answer!", e)
                            randomly_answered_questions.add((label_org, "text"))
                            answer = years_of_experience
                    else:
                        randomly_answered_questions.add((label_org, "text"))
                        answer = years_of_experience
                ##<
                text.clear()
                text.send_keys(answer)
                if do_actions:
                    sleep(2)
                    actions.send_keys(Keys.ARROW_DOWN)
                    actions.send_keys(Keys.ENTER).perform()
            questions_list.add((label, text.get_attribute("value"), "text", prev_answer))
            continue

        # Check if it's a textarea question
        text_area = try_xp(Question, ".//textarea", False)
        if text_area:
            label = try_xp(Question, ".//label[@for]", False)
            label_org = label.text if label else "Unknown"
            label = label_org.lower()
            answer = ""
            prev_answer = text_area.get_attribute("value")
            if not prev_answer or overwrite_previous_answers:
                if 'summary' in label: answer = linkedin_summary
                elif 'cover' in label: answer = cover_letter
                if answer == "":
                ##> ------ Yang Li : MARKYangL - Feature ------
                    if use_AI and aiClient:
                        try:
                            if ai_provider.lower() == "openai":
                                answer = ai_answer_question(aiClient, label_org, question_type="textarea", job_description=job_description, user_information_all=user_information_all)
                            elif ai_provider.lower() == "deepseek":
                                answer = deepseek_answer_question(aiClient, label_org, options=None, question_type="textarea", job_description=job_description, about_company=None, user_information_all=user_information_all)
                            elif ai_provider.lower() == "gemini":
                                answer = gemini_answer_question(aiClient, label_org, options=None, question_type="textarea", job_description=job_description, about_company=None, user_information_all=user_information_all)
                            else:
                                randomly_answered_questions.add((label_org, "textarea"))
                                answer = ""
                            if answer and isinstance(answer, str) and len(answer) > 0:
                                print_lg(f'AI Answered received for question "{label_org}" \nhere is answer: "{answer}"')
                            else:
                                randomly_answered_questions.add((label_org, "textarea"))
                                answer = ""
                        except Exception as e:
                            print_lg("Failed to get AI answer!", e)
                            randomly_answered_questions.add((label_org, "textarea"))
                            answer = ""
                    else:
                        randomly_answered_questions.add((label_org, "textarea"))
            text_area.clear()
            text_area.send_keys(answer)
            if do_actions:
                    sleep(2)
                    actions.send_keys(Keys.ARROW_DOWN)
                    actions.send_keys(Keys.ENTER).perform()
            questions_list.add((label, text_area.get_attribute("value"), "textarea", prev_answer))
            ##<
            continue

        # Check if it's a checkbox question
        checkbox = try_xp(Question, ".//input[@type='checkbox']", False)
        if checkbox:
            label = try_xp(Question, ".//span[@class='visually-hidden']", False)
            label_org = label.text if label else "Unknown"
            label = label_org.lower()
            answer = try_xp(Question, ".//label[@for]", False)  # Sometimes multiple checkboxes are given for 1 question, Not accounted for that yet
            answer = answer.text if answer else "Unknown"
            prev_answer = checkbox.is_selected()
            checked = prev_answer
            if not prev_answer:
                try:
                    actions.move_to_element(checkbox).click().perform()
                    checked = True
                except Exception as e: 
                    print_lg("Checkbox click failed!", e)
                    pass
            questions_list.add((f'{label} ([X] {answer})', checked, "checkbox", prev_answer))
            continue


    # Select todays date
    try_xp(driver, "//button[contains(@aria-label, 'This is today')]")

    # Collect important skills
    # if 'do you have' in label and 'experience' in label and ' in ' in label -> Get word (skill) after ' in ' from label
    # if 'how many years of experience do you have in ' in label -> Get word (skill) after ' in '

    return questions_list

def external_apply(
    pagination_element: WebElement,
    job_id: str,
    job_link: str,
    resume: str,
    date_listed,
    application_link: str,
    screenshot_name: str
) -> tuple[bool, str, int]:
    """
    Open external application link in a new tab, save the link, then return to LinkedIn and continue.

    Returns:
        (skip: bool, application_link: str, tabs_count: int)
        - skip=True means skip this job (e.g. daily limit reached / could not proceed)
        - application_link is the captured external URL (or previous value)
        - tabs_count = current number of tabs
    """
    global tabs_count, dailyEasyApplyLimitReached
    global failed_count

    try:
        # If user has set easy_apply_only=True, external apply is treated as "easy apply failed"
        if easy_apply_only:
            try:
                msg = driver.find_element(By.CLASS_NAME, "artdeco-inline-feedback__message").text
                if "exceeded the daily application limit" in msg:
                    dailyEasyApplyLimitReached = True
            except Exception:
                pass

            print_lg("Easy apply failed I guess!")
            # If we're in paginated results and easy apply only is enabled, just skip
            if pagination_element is not None:
                windows = driver.window_handles
                tabs_count = len(windows)
                return True, application_link, tabs_count

            application_link = "Not Available"

        # Click "Apply" button (this covers external apply too)
        wait.until(
            EC.element_to_be_clickable(
                (By.XPATH, ".//button[contains(@class,'jobs-apply-button') and contains(@class,'artdeco-button--3')]")
            )
        ).click()

        # Sometimes LinkedIn shows a "Continue" button before opening external tab
        try:
            wait_span_click(driver, "Continue", 2, True, False)
        except Exception:
            pass

        # Capture window handles
        windows = driver.window_handles
        tabs_count = len(windows)

        # Switch to newest tab (external site)
        driver.switch_to.window(windows[-1])

        # Get external URL
        application_link = driver.current_url
        print_lg(f'Got the external application link "{application_link}"')

        # IMPORTANT: Do not pause here. Keep automation running.
        # If close_tabs=True, close the external tab to avoid too many tabs.
        if close_tabs and driver.current_window_handle != linkedIn_tab:
            try:
                driver.close()
            except Exception:
                pass

        # Switch back to LinkedIn tab
        driver.switch_to.window(linkedIn_tab)

        return False, application_link, tabs_count

    except Exception as e:
        print_lg("Failed to apply!")
        failed_job(
            job_id,
            job_link,
            resume,
            date_listed,
            "Probably didn't find Apply button or unable to switch tabs.",
            e,
            application_link,
            screenshot_name
        )
        failed_count += 1

        # Try to get tabs count safely
        try:
            windows = driver.window_handles
            tabs_count = len(windows)
        except Exception:
            pass

        return True, application_link, tabs_count



def follow_company(modal: WebDriver = driver) -> None:
    '''
    Function to follow or un-follow easy applied companies based om `follow_companies`
    '''
    try:
        follow_checkbox_input = try_xp(modal, ".//input[@id='follow-company-checkbox' and @type='checkbox']", False)
        if follow_checkbox_input and follow_checkbox_input.is_selected() != follow_companies:
            try_xp(modal, ".//label[@for='follow-company-checkbox']")
    except Exception as e:
        print_lg("Failed to update follow companies checkbox!", e)
    


#< Failed attempts logging
def failed_job(job_id: str, job_link: str, resume: str, date_listed, error: str, exception: Exception, application_link: str, screenshot_name: str) -> None:
    '''
    Function to update failed jobs list in excel
    '''
    try:
        with open(failed_file_name, 'a', newline='', encoding='utf-8') as file:
            fieldnames = ['Job ID', 'Job Link', 'Resume Tried', 'Date listed', 'Date Tried', 'Assumed Reason', 'Stack Trace', 'External Job link', 'Screenshot Name']
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            if file.tell() == 0: writer.writeheader()
            writer.writerow({'Job ID':truncate_for_csv(job_id), 'Job Link':truncate_for_csv(job_link), 'Resume Tried':truncate_for_csv(resume), 'Date listed':truncate_for_csv(date_listed), 'Date Tried':datetime.now(), 'Assumed Reason':truncate_for_csv(error), 'Stack Trace':truncate_for_csv(exception), 'External Job link':truncate_for_csv(application_link), 'Screenshot Name':truncate_for_csv(screenshot_name)})
            file.close()
    except Exception as e:
        print_lg("Failed to update failed jobs list!", e)
        show_alert("Failed to update the excel of failed jobs!\nProbably because of 1 of the following reasons:\n1. The file is currently open or in use by another program\n2. Permission denied to write to the file\n3. Failed to find the file", "Failed Logging")


def screenshot(driver: WebDriver, job_id: str, failedAt: str) -> str:
    '''
    Function to to take screenshot for debugging
    - Returns screenshot name as String
    '''
    screenshot_dir = resolve_screenshot_directory()
    screenshot_dir.mkdir(parents=True, exist_ok=True)
    screenshot_name = build_serial_screenshot_name(screenshot_dir, job_id, failedAt)
    path = screenshot_dir / screenshot_name
    driver.save_screenshot(str(path))
    return screenshot_name
#>



APPLIED_JOBS_FIELDNAMES = [
    'Date',
    'Company Name',
    'Position',
    'Job Link',
    'Submitted',
    'HR Name',
    'HR Position',
    'HR Profile Link',
]

RECRUITER_FIELDNAMES = [
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

EXTERNAL_JOBS_FIELDNAMES = [
    'Date',
    'Company Name',
    'Position',
    'External Job Link',
    'HR Name',
    'HR Profile Link',
]


def clean_csv_text(value) -> str:
    '''
    Convert CSV values to compact, readable text.
    '''
    if value is None:
        return ""

    if isinstance(value, (set, list, tuple, dict)):
        return ""

    if isinstance(value, datetime):
        return format_applied_date(value)

    cleaned = str(value).replace("\r", " ").replace("\n", " ").strip()
    if cleaned in {"Unknown", "Pending", "In Development", "Needs an AI", "Not Available"}:
        return ""

    return truncate_for_csv(re.sub(r"\s+", " ", cleaned))


def get_logged_at_value(date_applied: datetime | Literal['Pending']) -> str:
    '''
    Use the actual applied timestamp when available, otherwise log the current time.
    '''
    if isinstance(date_applied, datetime):
        return format_applied_date(date_applied)
    return format_applied_date()


def capture_application_screenshot(job_id: str, stage_label: str) -> str:
    '''
    Capture a non-fatal screenshot during the Easy Apply happy path.
    '''
    try:
        return screenshot(driver, job_id, stage_label)
    except Exception as error:
        print_lg(f'Unable to capture "{stage_label}" screenshot for job {job_id}.', error)
        return ""


def resolve_screenshot_directory() -> pathlib.Path:
    '''
    In pipeline mode, always use the shared pipeline screenshots root.
    In manual mode, keep the configured/local screenshot folder behavior.
    '''
    if pipeline_mode:
        configured_dir = os.environ.get("PIPELINE_SCREENSHOTS_DIR", "").strip()
        if configured_dir:
            return pathlib.Path(configured_dir).expanduser()

        logs_path = pathlib.Path(logs_folder_path).expanduser()
        return logs_path.parent / "screenshots"

    return pathlib.Path(screenshot_folder_path).expanduser()


def screenshot_directory_is_run_local(screenshot_dir: pathlib.Path) -> bool:
    '''
    Detect accidental fallback into a run-local screenshots directory in pipeline mode.
    '''
    try:
        logs_path = pathlib.Path(logs_folder_path).expanduser().resolve()
        screenshot_path = screenshot_dir.expanduser().resolve()
    except Exception:
        return False
    return screenshot_path == logs_path / "screenshots"


def normalize_screenshot_label(label: str) -> str:
    '''
    Convert stage labels like "Before Apply" into filename-safe snake_case.
    '''
    normalized = re.sub(r"[^a-z0-9]+", "_", str(label or "").strip().lower())
    return normalized.strip("_") or "screenshot"


def sanitize_screenshot_token(value: str, fallback: str) -> str:
    '''
    Keep filename tokens short, readable, and safe for Windows paths.
    '''
    cleaned = re.sub(r"[^a-zA-Z0-9]+", "_", str(value or "").strip())
    cleaned = cleaned.strip("_")
    return (cleaned[:80] if cleaned else fallback)


def next_screenshot_serial(screenshot_dir: pathlib.Path) -> int:
    '''
    Scan the shared screenshot folder and return the next global counter.
    '''
    highest_serial = 0
    for image_path in screenshot_dir.glob("*.png"):
        match = re.match(r"(\d{4,})_", image_path.name)
        if match:
            highest_serial = max(highest_serial, int(match.group(1)))
    return highest_serial + 1


def build_serial_screenshot_name(screenshot_dir: pathlib.Path, job_id: str, stage_label: str) -> str:
    '''
    Build a globally sortable screenshot filename for the shared screenshot folder.
    '''
    serial = next_screenshot_serial(screenshot_dir)
    stage_token = normalize_screenshot_label(stage_label)
    job_token = sanitize_screenshot_token(job_id, "job")
    return f"{serial:04d}_{stage_token}_{job_token}.png"


def merge_csv_history(existing_value: str, new_value: str) -> str:
    '''
    Keep a comma-separated history without duplicating repeated values.
    '''
    values: list[str] = []
    for raw_value in (existing_value, new_value):
        for piece in str(raw_value or "").split(","):
            cleaned_piece = clean_csv_text(piece)
            if cleaned_piece and cleaned_piece not in values:
                values.append(cleaned_piece)
    return ", ".join(values)


def merge_applied_job_rows(existing_row: dict[str, str], new_row: dict[str, str]) -> dict[str, str]:
    '''
    Merge a repeat application into the same CSV row so date history keeps growing.
    '''
    merged_row = {field: clean_csv_text(existing_row.get(field, "")) for field in APPLIED_JOBS_FIELDNAMES}
    for field in ("Company Name", "Position", "HR Name", "HR Position", "HR Profile Link"):
        if clean_csv_text(new_row.get(field, "")):
            merged_row[field] = clean_csv_text(new_row.get(field, ""))

    merged_row["Job Link"] = clean_csv_text(new_row.get("Job Link", "")) or merged_row.get("Job Link", "")
    merged_row["Date"] = merge_csv_history(existing_row.get("Date", ""), new_row.get("Date", ""))
    merged_row["Submitted"] = merge_csv_history(existing_row.get("Submitted", ""), new_row.get("Submitted", ""))
    return merged_row


def upsert_applied_job_row(new_row: dict[str, str]) -> bool:
    '''
    Update an existing applied job row by Job Link or append a new one.
    '''
    clean_job_link = clean_csv_text(new_row.get("Job Link", ""))
    if not clean_job_link:
        print_lg("Skipping applied job logging because the LinkedIn job link is missing.")
        return False

    existing_rows: list[dict[str, str]] = []
    row_updated = False

    if os.path.exists(file_name):
        with open(file_name, 'r', encoding='utf-8-sig', newline='') as csv_file:
            reader = csv.DictReader(csv_file)
            for row in reader:
                normalized_row = {field: clean_csv_text(row.get(field, "")) for field in APPLIED_JOBS_FIELDNAMES}
                row_job_link = clean_csv_text(normalized_row.get("Job Link", ""))
                if row_job_link and row_job_link == clean_job_link:
                    existing_rows.append(merge_applied_job_rows(normalized_row, new_row))
                    row_updated = True
                elif any(normalized_row.values()):
                    existing_rows.append(normalized_row)

    if not row_updated:
        existing_rows.append({field: clean_csv_text(new_row.get(field, "")) for field in APPLIED_JOBS_FIELDNAMES})

    with open(file_name, 'w', newline='', encoding='utf-8') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=APPLIED_JOBS_FIELDNAMES)
        writer.writeheader()
        writer.writerows(existing_rows)
    return True


def warn_applied_csv_locked() -> None:
    '''
    Surface a clear warning when the applied CSV is open in Excel or otherwise locked.
    '''
    global applied_csv_lock_warned
    warning_message = (
        f"Applied jobs CSV is locked: {file_name}\n\n"
        "Close the CSV/Excel file before running or while the bot is trying to save applied jobs."
    )
    print_lg(warning_message)
    if not applied_csv_lock_warned:
        applied_csv_lock_warned = True
        show_alert(warning_message, "Applied CSV Locked", "OK")


def warn_external_jobs_csv_locked() -> None:
    '''
    Surface a clear warning when the external jobs CSV is open in Excel or otherwise locked.
    '''
    global external_csv_lock_warned
    warning_message = (
        f"External jobs CSV is locked: {external_jobs_file_name}\n\n"
        "Close the CSV/Excel file before running or while the bot is trying to save external jobs."
    )
    print_lg(warning_message)
    if not external_csv_lock_warned:
        external_csv_lock_warned = True
        show_alert(warning_message, "External CSV Locked", "OK")


def warn_recruiter_csv_locked() -> None:
    warning_message = (
        f"Recruiter CSV is locked: {recruiters_file_name}\n\n"
        "Close the recruiter CSV/Excel file while the bot is updating RocketReach output."
    )
    print_lg(warning_message)


def ensure_recruiters_csv_schema() -> bool:
    try:
        csv_folder = os.path.dirname(recruiters_file_name)
        if csv_folder:
            os.makedirs(csv_folder, exist_ok=True)

        expected_header = ",".join(RECRUITER_FIELDNAMES)
        rewrite_file = False
        if os.path.exists(recruiters_file_name):
            with open(recruiters_file_name, 'r', encoding='utf-8', newline='') as csv_file:
                first_line = csv_file.readline().strip()
                rewrite_file = first_line != expected_header

        if rewrite_file or not os.path.exists(recruiters_file_name):
            with open(recruiters_file_name, 'w', newline='', encoding='utf-8') as csv_file:
                writer = csv.DictWriter(csv_file, fieldnames=RECRUITER_FIELDNAMES)
                writer.writeheader()
        return True
    except PermissionError:
        warn_recruiter_csv_locked()
        return False


def _load_rocketreach_module():
    module_path = pathlib.Path(__file__).resolve().parents[1] / 'rocket_reach - testing' / 'rocketreach_bulk.py'
    if not module_path.exists():
        return None

    module_name = '_local_rocketreach_bulk'
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    if spec is None or spec.loader is None:
        return None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _write_placeholder_recruiter_csv() -> None:
    if not ensure_recruiters_csv_schema():
        return

    rows = []
    with open(file_name, 'r', encoding='utf-8-sig', newline='') as source:
        reader = csv.DictReader(source)
        for row in reader:
            rows.append({
                'Date': (row.get('Date') or '').strip(),
                'Company Name': (row.get('Company Name') or '').strip(),
                'Position': (row.get('Position') or '').strip(),
                'Job Link': (row.get('Job Link') or '').strip(),
                'Submitted': (row.get('Submitted') or '').strip(),
                'HR Name': (row.get('HR Name') or '').strip(),
                'HR Position': (row.get('HR Position') or '').strip(),
                'HR Profile Link': (row.get('HR Profile Link') or '').strip(),
                'HR Email': '',
                'HR Secondary Email': '',
                'HR Email Preview': '',
                'HR Contact': '',
                'HR Contact Preview': '',
                'RocketReach Status': 'pending_enrichment',
            })

    with open(recruiters_file_name, 'w', encoding='utf-8', newline='') as target:
        writer = csv.DictWriter(target, fieldnames=RECRUITER_FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)


def sync_recruiter_csv_after_application() -> None:
    try:
        _write_placeholder_recruiter_csv()
    except PermissionError:
        warn_recruiter_csv_locked()
        return
    except Exception as error:
        print_lg('Failed to write placeholder recruiter CSV.', error)
        return

    try:
        rr = _load_rocketreach_module()
        if rr is None:
            return
        rr.bulk_enrich(file_name, recruiters_file_name)
    except PermissionError:
        warn_recruiter_csv_locked()
    except Exception as error:
        print_lg('RocketReach sync after application failed. Placeholder recruiter CSV was kept.', error)


def ensure_applied_jobs_csv_schema() -> bool:
    '''
    Ensure the applied jobs CSV exists and matches the clean schema.
    '''
    try:
        csv_folder = os.path.dirname(file_name)
        if csv_folder:
            os.makedirs(csv_folder, exist_ok=True)

        expected_header = ",".join(APPLIED_JOBS_FIELDNAMES)
        rewrite_file = False

        if os.path.exists(file_name):
            with open(file_name, 'r', encoding='utf-8', newline='') as csv_file:
                first_line = csv_file.readline().strip()
                rewrite_file = first_line != expected_header

        if rewrite_file or not os.path.exists(file_name):
            with open(file_name, 'w', newline='', encoding='utf-8') as csv_file:
                writer = csv.DictWriter(csv_file, fieldnames=APPLIED_JOBS_FIELDNAMES)
                writer.writeheader()
        return True
    except PermissionError:
        warn_applied_csv_locked()
        return False


def ensure_external_jobs_csv_schema() -> bool:
    '''
    Ensure the external jobs CSV exists and matches the clean schema.
    '''
    try:
        csv_folder = os.path.dirname(external_jobs_file_name)
        if csv_folder:
            os.makedirs(csv_folder, exist_ok=True)

        expected_header = ",".join(EXTERNAL_JOBS_FIELDNAMES)
        rewrite_file = False

        if os.path.exists(external_jobs_file_name):
            with open(external_jobs_file_name, 'r', encoding='utf-8', newline='') as csv_file:
                first_line = csv_file.readline().strip()
                rewrite_file = first_line != expected_header

        if rewrite_file or not os.path.exists(external_jobs_file_name):
            with open(external_jobs_file_name, 'w', newline='', encoding='utf-8') as csv_file:
                writer = csv.DictWriter(csv_file, fieldnames=EXTERNAL_JOBS_FIELDNAMES)
                writer.writeheader()
        return True
    except PermissionError:
        warn_external_jobs_csv_locked()
        return False


def external_job_already_logged(job_link: str) -> bool:
    '''
    Detect duplicates in the current run using the LinkedIn job link as the stable key.
    '''
    return clean_csv_text(job_link) in logged_external_job_links


def log_external_job(company: str, title: str, job_link: str, application_link: str, hr_name: str, hr_link: str) -> bool:
    '''
    Save a collected external job to the dedicated CSV and skip duplicates by LinkedIn job link.
    '''
    global rows_written_to_external_csv, logged_external_job_links
    try:
        if not ensure_external_jobs_csv_schema():
            return False

        clean_job_link = clean_csv_text(job_link)
        if not clean_job_link:
            print_lg("Skipping external job logging because LinkedIn job link is missing.")
            return False

        if external_job_already_logged(clean_job_link):
            print_lg(f'External job already logged for "{clean_csv_text(title)} | {clean_csv_text(company)}". Skipping duplicate row.')
            return False

        with open(external_jobs_file_name, mode='a', newline='', encoding='utf-8') as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=EXTERNAL_JOBS_FIELDNAMES)
            if csv_file.tell() == 0:
                writer.writeheader()
            writer.writerow({
                'Date': get_logged_at_value("Pending"),
                'Company Name': clean_csv_text(company),
                'Position': clean_csv_text(title),
                'External Job Link': clean_csv_text(application_link),
                'HR Name': clean_csv_text(hr_name),
                'HR Profile Link': clean_csv_text(hr_link),
            })
        logged_external_job_links.add(clean_job_link)
        rows_written_to_external_csv += 1
        return True
    except PermissionError:
        warn_external_jobs_csv_locked()
        return False
    except Exception as e:
        print_lg("Failed to update external jobs list!", e)
        show_alert(
            "Failed to update the excel of external jobs!\nProbably because of 1 of the following reasons:\n1. The file is currently open or in use by another program\n2. Permission denied to write to the file\n3. Failed to find the file",
            "Failed External Logging",
        )
        return False


def extract_hr_position(hr_info_card: WebElement, hr_name: str) -> str:
    '''
    Capture the recruiter title from the card without guessing from unrelated text.
    '''
    card_lines = [line.strip() for line in hr_info_card.text.splitlines() if line.strip()]
    ignored_fragments = ("connection", "follower", "message", "connect", "follow", "job poster", "hiring team")

    for line in card_lines:
        lowered = line.lower()
        if clean_csv_text(line) == clean_csv_text(hr_name):
            continue
        if any(fragment in lowered for fragment in ignored_fragments):
            continue
        return clean_csv_text(line)

    return ""


def normalize_linkedin_profile_link(url: str | None) -> str:
    '''
    Keep only trustworthy LinkedIn person-profile links.
    '''
    if not url:
        return ""

    parsed = urlparse(url.strip())
    netloc = parsed.netloc.lower()
    path = parsed.path.rstrip('/')
    normalized_path = path.lower()
    profile_slug = normalized_path.split('/')[-1] if normalized_path else ''
    blocked_profile_slugs = {
        '',
        'copyright',
        'feed',
        'help',
        'jobs',
        'learning',
        'privacy',
        'sales',
    }
    if 'linkedin.com' not in netloc:
        return ""
    if not (normalized_path.startswith('/in/') or normalized_path.startswith('/pub/')):
        return ""
    if profile_slug in blocked_profile_slugs:
        return ""
    scheme = parsed.scheme or 'https'
    return f"{scheme}://{parsed.netloc}{path}"


def extract_hr_details_from_card(card: WebElement) -> tuple[str, str, str]:
    '''
    Extract recruiter name, profile link, and title from a visible recruiter-style card.
    '''
    profile_link = ""
    profile_name = ""

    for anchor in card.find_elements(By.XPATH, ".//a[@href]"):
        normalized_link = normalize_linkedin_profile_link(anchor.get_attribute('href'))
        if normalized_link:
            profile_link = normalized_link
            profile_name = clean_csv_text(anchor.text)
            if profile_name:
                break

    card_lines = [clean_csv_text(line) for line in card.text.splitlines() if clean_csv_text(line)]
    if not profile_name:
        for line in card_lines:
            lowered = line.lower()
            if any(fragment in lowered for fragment in ("message", "connect", "follow", "job poster", "hiring team")):
                continue
            profile_name = line
            break

    if not profile_name and not profile_link:
        return "", "", ""

    profile_position = extract_hr_position(card, profile_name)
    return profile_name, profile_link, profile_position


def extract_hr_details() -> tuple[str, str, str]:
    '''
    Find recruiter details from explicit recruiter/person cards only.
    '''
    card_selectors = [
        "//div[contains(@class, 'hirer-card')]",
        "//section[contains(@class, 'jobs-poster')]",
        "//div[contains(@class, 'jobs-poster')]",
        "//section[contains(@class, 'job-details-people-who-can-help')]",
        "//div[contains(@class, 'job-details-people-who-can-help')]",
        "//section[contains(@class, 'hiring-team')]",
        "//div[contains(@class, 'hiring-team')]",
    ]

    seen_links = set()
    for selector in card_selectors:
        for card in driver.find_elements(By.XPATH, selector):
            try:
                hr_name, hr_link, hr_position = extract_hr_details_from_card(card)
            except StaleElementReferenceException:
                continue
            if not hr_name and not hr_link:
                continue
            if hr_link and hr_link in seen_links:
                continue
            if hr_link:
                seen_links.add(hr_link)
            return hr_name, hr_link, hr_position

    return "", "", ""


def application_sent_confirmation_present(root: WebElement | None = None) -> bool:
    '''
    Check LinkedIn's post-submit success state before treating the application as failed.
    '''
    success_selectors = [
        ".//h2[contains(normalize-space(), 'Application sent')]",
        ".//*[contains(normalize-space(), 'Your application was sent to')]",
        ".//*[contains(normalize-space(), 'Your application was sent')]",
        ".//button[normalize-space()='Done' or .//span[normalize-space()='Done']]",
    ]

    search_roots = [root] if root is not None else []
    search_roots.append(driver)

    for search_root in search_roots:
        for selector in success_selectors:
            try:
                elements = search_root.find_elements(By.XPATH, selector)
                if any(element.is_displayed() for element in elements):
                    return True
            except Exception:
                continue
    return False


def confirmed_easy_apply_submission(timeout: float = 3.5) -> bool:
    '''
    Re-check the modal/page for LinkedIn success markers before treating submit as failed.
    '''
    deadline = time.time() + timeout
    while time.time() < deadline:
        modal = get_active_easy_apply_modal(0.5)
        if application_sent_confirmation_present(modal):
            return True
        time.sleep(0.2)
    return application_sent_confirmation_present()


def close_easy_apply_success_dialog() -> None:
    '''
    Dismiss the post-submit success dialog when it is visible.
    '''
    if click_easy_apply_button(["Done"], timeout=2.0, scroll_top=True):
        return
    try:
        actions.send_keys(Keys.ESCAPE).perform()
    except Exception:
        pass


def submitted_jobs(job_id: str, title: str, company: str, work_location: str, work_style: str, description: str, experience_required: int | Literal['Unknown', 'Error in extraction'], 
                   skills: list[str] | Literal['In Development'], hr_name: str | Literal['Unknown'], hr_link: str | Literal['Unknown'], hr_position: str, resume: str, 
                   reposted: bool, date_listed: datetime | Literal['Unknown'], date_applied:  datetime | Literal['Pending'], job_link: str, application_link: str, 
                   questions_list: set | None, connect_request: Literal['In Development']) -> bool:
    '''
    Function to create or update the Applied jobs CSV file, once the application is submitted successfully
    '''
    global rows_written_to_applied_csv, rows_missing_hr_profile
    try:
        if not ensure_applied_jobs_csv_schema():
            return False

        clean_hr_link = clean_csv_text(hr_link)
        if not clean_hr_link or clean_hr_link.lower() == 'unknown':
            rows_missing_hr_profile += 1

        saved = upsert_applied_job_row({
            'Date': get_logged_at_value(date_applied),
            'Company Name': clean_csv_text(company),
            'Position': clean_csv_text(title),
            'Job Link': clean_csv_text(job_link),
            'Submitted': 'Applied',
            'HR Name': clean_csv_text(hr_name),
            'HR Position': clean_csv_text(hr_position),
            'HR Profile Link': clean_hr_link,
        })
        if not saved:
            return False
        sync_recruiter_csv_after_application()
        rows_written_to_applied_csv += 1
        return True
    except PermissionError:
        warn_applied_csv_locked()
        return False
    except Exception as e:
        print_lg("Failed to update submitted jobs list!", e)
        show_alert("Failed to update the excel of applied jobs!\nProbably because of 1 of the following reasons:\n1. The file is currently open or in use by another program\n2. Permission denied to write to the file\n3. Failed to find the file", "Failed Logging")
        return False


# Function to discard the job application
def discard_job() -> bool:
    wait_for_transient_overlays_to_clear(1.5)
    try:
        actions.send_keys(Keys.ESCAPE).perform()
    except Exception:
        pass

    if click_easy_apply_button(["Discard"], timeout=2.0, scroll_top=True):
        return True

    try:
        return bool(wait_span_click(driver, 'Discard', 1))
    except Exception:
        return False


# Function to apply to jobs
def apply_to_jobs(search_terms: list[str]) -> None:
    applied_jobs = get_applied_job_ids()
    rejected_jobs = set()
    blacklisted_companies = set()
    global current_city, failed_count, skip_count, easy_applied_count, external_jobs_count, tabs_count, pause_before_submit, pause_at_failed_question, useNewResume, dailyEasyApplyLimitReached
    current_city = current_city.strip()

    if randomize_search_order:  shuffle(search_terms)
    for searchTerm in search_terms:
        driver.get(f"https://www.linkedin.com/jobs/search/?keywords={searchTerm}")
        print_lg("\n________________________________________________________________________________________________________________________\n")
        print_lg(f'\n>>>> Now searching for "{searchTerm}" <<<<\n\n')
        apply_filters()

        current_count = 0
        try:
            while current_count < switch_number:
                # Wait until job listings are loaded
                wait.until(EC.presence_of_all_elements_located((By.XPATH, "//li[@data-occludable-job-id]")))

                pagination_element, current_page = get_page_info()

                # Find all job listings in current page
                buffer(3)
                page_job_ids = get_job_listing_ids()

                for listed_job_id in page_job_ids:
                    if keep_screen_awake: pyautogui.press('shiftright')
                    if current_count >= switch_number: break
                    print_lg("\n-@-\n")

                    stale_retry = 0
                    skip = False
                    while stale_retry < 2:
                        try:
                            job = find_job_card_by_id(listed_job_id)
                            job_id, title, company, work_location, work_style, skip = get_job_main_details(job, blacklisted_companies, rejected_jobs)
                            break
                        except StaleElementReferenceException as e:
                            stale_retry += 1
                            print_lg(f"Recoverable stale element while opening job {listed_job_id}. Retry {stale_retry}/2", e)
                            buffer(click_gap)
                            if stale_retry >= 2:
                                print_lg(f"Skipping job {listed_job_id} after repeated stale element errors.")
                                skip_count += 1
                                skip = True
                    if skip:
                        continue

                    job_link = "https://www.linkedin.com/jobs/view/"+job_id
                    try:
                        if job_link in applied_jobs or find_by_class(driver, "jobs-s-apply__application-link", 2):
                            print_lg(f'Already applied to "{title} | {company}" job. Job ID: {job_id}!')
                            continue
                    except StaleElementReferenceException as e:
                        print_lg(f"Recoverable stale element while checking applied state for {job_id}. Continuing.", e)
                    except Exception:
                        print_lg(f'Job card opened: "{title} | {company}". Job ID: {job_id}')

                    application_link = "Easy Applied"
                    application_submitted = False
                    date_applied = "Pending"
                    hr_link = ""
                    hr_name = ""
                    hr_position = ""
                    connect_request = "In Development"
                    date_listed = "Unknown"
                    skills = "Needs an AI"
                    resume = "Pending"
                    reposted = False
                    questions_list = None
                    screenshot_name = "Not Available"

                    try:
                        rejected_jobs, blacklisted_companies, jobs_top_card = check_blacklist(rejected_jobs, job_id, company, blacklisted_companies)
                    except ValueError as e:
                        print_lg(e, 'Skipping this job!\n')
                        failed_job(job_id, job_link, resume, date_listed, "Found Blacklisted words in About Company", e, "Skipped", screenshot_name)
                        skip_count += 1
                        continue
                    except StaleElementReferenceException as e:
                        print_lg(f"Recoverable stale element while loading job details for {job_id}. Skipping job.", e)
                        skip_count += 1
                        continue
                    except Exception:
                        print_lg("Failed to scroll to About Company!")

                    try:
                        hr_name, hr_link, hr_position = extract_hr_details()
                        if not (hr_name or hr_link):
                            print_lg(f'HR info was not given for "{title}" with Job ID: {job_id}!')
                    except StaleElementReferenceException as e:
                        print_lg(f"Recoverable stale element while reading HR info for {job_id}. Leaving HR fields blank.", e)
                    except Exception as e:
                        print_lg(f'Failed to extract HR info for "{title}" with Job ID: {job_id}!', e)

                    try:
                        time_posted_text = jobs_top_card.find_element(By.XPATH, './/span[contains(normalize-space(), " ago")]').text
                        if "Reposted" in time_posted_text:
                            reposted = True
                            time_posted_text = time_posted_text.replace("Reposted", "")
                        date_listed = calculate_date_posted(time_posted_text.strip())
                    except Exception as e:
                        print_lg("Failed to calculate the date posted!", e)

                    description, experience_required, skip, reason, message = get_job_description()
                    if skip:
                        print_lg(message)
                        failed_job(job_id, job_link, resume, date_listed, reason, message, "Skipped", screenshot_name)
                        rejected_jobs.add(job_id)
                        skip_count += 1
                        continue

                    if use_AI and description != "Unknown":
                        try:
                            if ai_provider.lower() == "openai":
                                skills = ai_extract_skills(aiClient, description)
                            elif ai_provider.lower() == "deepseek":
                                skills = deepseek_extract_skills(aiClient, description)
                            elif ai_provider.lower() == "gemini":
                                skills = gemini_extract_skills(aiClient, description)
                            else:
                                skills = "In Development"
                            print_lg(f"Extracted skills using {ai_provider} AI")
                        except Exception as e:
                            print_lg("Failed to extract skills:", e)
                            skills = "Error extracting skills"

                    uploaded = False
                    if try_xp(driver, EASY_APPLY_BUTTON_XPATH, False):
                        try:
                            errored = ""
                            resume = "Previous resume"
                            questions_list = set()
                            next_counter = 0

                            modal, before_apply_screenshot_name = launch_easy_apply(job_id)
                            if not modal:
                                print_lg(f'Job "{title} | {company}" does not expose an Easy Apply button on the page. Skipping.')
                                skip_count += 1
                                continue
                            
                            print_lg(f"Easy Apply clicked for {title}. Starting application flow...")

                            while True:
                                next_counter += 1
                                if next_counter >= 15:
                                    if pause_at_failed_question:
                                        show_alert("Couldn't answer one or more questions.\nPlease click \"Continue\" once done.\nDO NOT CLICK Back, Next or Review button in LinkedIn.\n\n\n\n\nYou can turn off \"Pause at failed question\" setting in config.py", "Help Needed", "Continue")
                                        next_counter = 1
                                        continue
                                    if questions_list:
                                        print_lg("Stuck for one or some of the following questions...", questions_list)
                                    screenshot_name = screenshot(driver, job_id, "Failed at questions")
                                    errored = "stuck"
                                    raise Exception("Seems like stuck in a continuous loop of next/review, probably because of new questions.")

                                modal = get_active_easy_apply_modal()
                                if not modal:
                                    break

                                questions_list = answer_questions(modal, questions_list, work_location, job_description=description)
                                if useNewResume and not uploaded:
                                    uploaded, resume = upload_resume(modal, default_resume_path)
                                easy_apply_step_buffer()

                                needs_manual_pause, pause_reason = detect_manual_form_needs(modal)
                                if needs_manual_pause:
                                    pause_for_manual_form_completion(job_id, pause_reason)
                                    next_counter = 0
                                    easy_apply_step_buffer()
                                    continue

                                if click_easy_apply_button(["Review"], timeout=1.2, scroll_top=True):
                                    easy_apply_step_buffer()
                                    break
                                if click_easy_apply_button(["Next"], timeout=1.2, scroll_top=True):
                                    easy_apply_step_buffer()
                                    continue
                                if find_easy_apply_button(["Submit application"], timeout=0.8) or application_sent_confirmation_present(modal):
                                    break

                                screenshot_name = screenshot(driver, job_id, "Unable to advance Easy Apply")
                                raise Exception("Could not find Next, Review, or Submit Application in Easy Apply modal")

                            if questions_list and errored != "stuck":
                                print_lg("Answered the following questions...", questions_list)
                                print("\n\n" + "\n".join(str(question) for question in questions_list) + "\n\n")

                            easy_apply_step_buffer()
                            modal = get_active_easy_apply_modal(1.0)
                            if modal and not application_sent_confirmation_present(modal):
                                click_easy_apply_button(["Review"], timeout=1.0, scroll_top=True)
                                modal = get_active_easy_apply_modal(1.0) or modal

                            cur_pause_before_submit = pause_before_submit
                            if errored != "stuck" and cur_pause_before_submit and not application_sent_confirmation_present(modal):
                                decision = show_confirm('1. Please verify your information.\n2. If you edited something, please return to this final screen.\n3. DO NOT CLICK "Submit Application".\n\n\n\n\nYou can turn off "Pause before submit" setting in config.py\nTo TEMPORARILY disable pausing, click "Disable Pause"', "Confirm your information", ["Disable Pause", "Discard Application", "Submit Application"])
                                if decision == "Discard Application":
                                    raise Exception("Job application discarded by user!")
                                pause_before_submit = False if "Disable Pause" == decision else True

                            modal = get_active_easy_apply_modal(1.0)
                            if modal:
                                follow_company(modal)
                            easy_apply_step_buffer()
                            if click_easy_apply_button(["Submit application"], timeout=2.5, scroll_top=True):
                                easy_apply_step_buffer()
                                if confirmed_easy_apply_submission(3.5):
                                    date_applied = datetime.now()
                                    application_submitted = True
                                    print_lg(f"Application submitted successfully for {title} | {company}")
                                    capture_application_screenshot(job_id, "After Submitted")
                                    close_easy_apply_success_dialog()
                            elif confirmed_easy_apply_submission(3.0):
                                date_applied = datetime.now()
                                application_submitted = True
                                capture_application_screenshot(job_id, "After Submitted")
                                close_easy_apply_success_dialog()
                            elif errored != "stuck" and cur_pause_before_submit and "Yes" in show_confirm("You submitted the application, didn't you?", "Failed to find Submit Application!", ["Yes", "No"]):
                                date_applied = datetime.now()
                                application_submitted = True
                                capture_application_screenshot(job_id, "After Submitted")
                                close_easy_apply_success_dialog()
                            else:
                                print_lg("Since, Submit Application failed, discarding the job application...")
                                if before_apply_screenshot_name:
                                    print_lg(f'Before Apply screenshot captured: {before_apply_screenshot_name}')
                                raise Exception("Application was not confirmed as submitted")

                        except StaleElementReferenceException as e:
                            print_lg(f"Recoverable stale element during Easy Apply for {job_id}. Skipping this job.", e)
                            skip_count += 1
                            continue
                        except Exception as e:
                            if application_submitted or confirmed_easy_apply_submission(1.5):
                                application_submitted = True
                                if not isinstance(date_applied, datetime):
                                    date_applied = datetime.now()
                                print_lg(f'Application submit was confirmed for "{title} | {company}" even though cleanup failed. Saving to applied history.')
                                close_easy_apply_success_dialog()
                            else:
                                print_lg("Failed to Easy apply!")
                                critical_error_log("Somewhere in Easy Apply process",e)
                                failed_job(job_id, job_link, resume, date_listed, "Problem in Easy Applying", e, application_link, screenshot_name)
                                failed_count += 1
                                discard_job()
                                continue
                    else:
                        skip, application_link, tabs_count = external_apply(pagination_element, job_id, job_link, resume, date_listed, application_link, screenshot_name)
                        if dailyEasyApplyLimitReached:
                            print_lg("\n###############  Daily application limit for Easy Apply is reached!  ###############\n")
                            return
                        if skip:
                            continue
                        saved_external_job = log_external_job(company, title, job_link, application_link, hr_name, hr_link)
                        if saved_external_job:
                            external_jobs_count += 1
                            print_lg(f'Saved external application row for "{title} | {company}" job.')
                        else:
                            print_lg(f'Collected external application link for "{title} | {company}" job, but no new external CSV row was written.')
                        current_count += 1
                        continue

                    if not application_submitted:
                        print_lg(f'Skipping applied-history save for "{title} | {company}" because the application was not confirmed submitted.')
                        continue

                    saved_to_history = submitted_jobs(job_id, title, company, work_location, work_style, description, experience_required, skills, hr_name, hr_link, hr_position, resume, reposted, date_listed, date_applied, job_link, application_link, questions_list, connect_request)
                    if uploaded:
                        useNewResume = False

                    if saved_to_history:
                        print_lg(f'Successfully saved "{title} | {company}" job. Job ID: {job_id} info')
                    else:
                        print_lg(f'Application for "{title} | {company}" was submitted but could not be written to applied history.')
                    current_count += 1
                    easy_applied_count += 1
                    applied_jobs.add(job_link)
                    if should_stop_after_configured_easy_apply_limit():
                        dailyEasyApplyLimitReached = True
                        print_lg(f"Configured Easy Apply limit reached at {easy_applied_count}/{pipeline_max_easy_apply}. Stopping LinkedIn stage.")
                        return

                # Switching to next page
                if pagination_element == None:
                    print_lg("Couldn't find pagination element, probably at the end page of results!")
                    break
                try:
                    pagination_element.find_element(By.XPATH, f"//button[@aria-label='Page {current_page+1}']").click()
                    print_lg(f"\n>-> Now on Page {current_page+1} \n")
                except NoSuchElementException:
                    print_lg(f"\n>-> Didn't find Page {current_page+1}. Probably at the end page of results!\n")
                    break

        except NoSuchWindowException as e:
            print_lg("Browser window closed or session is invalid. Ending application process.", e)
            raise e
        except WebDriverException as e:
            if is_session_invalid_error(e):
                print_lg("Browser window closed or session is invalid. Ending application process.", e)
                raise e
            print_lg("Recoverable webdriver issue while processing job listings. Continuing current run.", e)
        except Exception as e:
            print_lg("Failed to find Job listings!")
            critical_error_log("In Applier", e)
            try:
                print_lg(driver.page_source, pretty=True)
            except Exception as page_source_error:
                print_lg(f"Failed to get page source, browser might have crashed. {page_source_error}")
            # print_lg(e)

        
def run(total_runs: int) -> int:
    if dailyEasyApplyLimitReached:
        return total_runs
    print_lg("\n########################################################################################################################\n")
    print_lg(f"Date and Time: {datetime.now()}")
    print_lg(f"Cycle number: {total_runs}")
    if target_job_link.strip():
        print_lg(f"Running single target job flow for: {target_job_link.strip()}")
        apply_to_target_job(target_job_link.strip())
    else:
        print_lg(f"Currently looking for jobs posted within '{date_posted}' and sorting them by '{sort_by}'")
        print_lg(f"Job search started for terms: {search_terms}")
        apply_to_jobs(search_terms)
    print_lg("########################################################################################################################\n")
    if target_job_link.strip():
        return total_runs + 1
    if not dailyEasyApplyLimitReached and not pipeline_mode:
        print_lg("Sleeping for 10 min...")
        sleep(300)
        print_lg("Few more min... Gonna start with in next 5 min...")
        sleep(300)
    buffer(3)
    return total_runs + 1



chatGPT_tab = False
linkedIn_tab = False

def heartbeat_thread():
    while True:
        try:
            print_lg("Bot Heartbeat: Working...")
        except:
            pass
        time.sleep(20)

def main() -> dict[str, str | int | bool]:
    h_thread = threading.Thread(target=heartbeat_thread, daemon=True)
    h_thread.start()

    show_alert("Please consider sponsoring this project at:\n\nhttps://github.com/sponsors/GodsScion\n\n", "Support the project", "Okay")
    total_runs = 1
    session_end_reason = "Session ended."
    pipeline_failed = False
    try:
        global linkedIn_tab, tabs_count, useNewResume, aiClient, options, driver, actions, wait
        alert_title = "Error Occurred. Closing Browser!"
        validate_config()
        ensure_applied_jobs_csv_schema()
        ensure_external_jobs_csv_schema()
        resolved_screenshot_dir = resolve_screenshot_directory()
        print_lg(f"Screenshot directory: {resolved_screenshot_dir}")
        if pipeline_mode and screenshot_directory_is_run_local(resolved_screenshot_dir):
            raise RuntimeError(
                f"Pipeline screenshot directory must be shared, but resolved to run-local path: {resolved_screenshot_dir}"
            )
        
        if not os.path.exists(default_resume_path):
            show_alert(text='Your default resume "{}" is missing! Please update it\'s folder path "default_resume_path" in config.py\n\nOR\n\nAdd a resume with exact name and path (check for spelling mistakes including cases).\n\n\nFor now the bot will continue using your previous upload from LinkedIn!'.format(default_resume_path), title="Missing Resume", button="OK")
            useNewResume = False
        
        # Login to LinkedIn
        options, driver, actions, wait = initializeChromeSession()
        tabs_count = len(driver.window_handles)
        driver.get("https://www.linkedin.com/login")
        print_lg("LinkedIn opened. Checking login session...")
        if not is_logged_in_LN():
            print_lg("LinkedIn login session missing. Attempting login...")
            if not login_LN():
                pipeline_failed = True
                session_end_reason = "LinkedIn login was not confirmed. Complete manual login in Chrome and keep the browser window open."
            print_lg(session_end_reason)
            return {
                "exit_code": 1,
                "session_end_reason": session_end_reason,
                "unexpected_failure": True,
                **build_stage_summary(total_runs),
            }
        
        print_lg("Login detected. LinkedIn session is active and confirmed.")
        linkedIn_tab = driver.current_window_handle

        # # Login to ChatGPT in a new tab for resume customization
        # if use_resume_generator:
        #     try:
        #         driver.switch_to.new_window('tab')
        #         driver.get("https://chat.openai.com/")
        #         if not is_logged_in_GPT(): login_GPT()
        #         open_resume_chat()
        #         global chatGPT_tab
        #         chatGPT_tab = driver.current_window_handle
        #     except Exception as e:
        #         print_lg("Opening OpenAI chatGPT tab failed!")
        if use_AI:
            if ai_provider == "openai":
                aiClient = ai_create_openai_client()
            ##> ------ Yang Li : MARKYangL - Feature ------
            # Create DeepSeek client
            elif ai_provider == "deepseek":
                aiClient = deepseek_create_client()
            elif ai_provider == "gemini":
                aiClient = gemini_create_client()
            ##<

            try:
                about_company_for_ai = " ".join([word for word in (first_name+" "+last_name).split() if len(word) > 3])
                print_lg(f"Extracted about company info for AI: '{about_company_for_ai}'")
            except Exception as e:
                print_lg("Failed to extract about company info!", e)
        
        # Start applying to jobs
        driver.switch_to.window(linkedIn_tab)
        while True:
            try:
                total_runs = run(total_runs)
            except WebDriverException as e:
                if is_session_invalid_error(e):
                    raise e
                session_end_reason = "Stopped because of a recoverable webdriver issue."
                print_lg("Recoverable webdriver issue reached main loop. Continuing with next cycle.", e)
                buffer(2)
                continue

            if dailyEasyApplyLimitReached:
                if pipeline_mode and pipeline_max_easy_apply > 0 and easy_applied_count >= pipeline_max_easy_apply:
                    session_end_reason = f"Configured Easy Apply limit reached at {easy_applied_count}/{pipeline_max_easy_apply}."
                else:
                    session_end_reason = "Daily Easy Apply limit reached."
                break
            if target_job_link.strip():
                session_end_reason = "Completed target job apply pass."
                break
            if should_run_single_pipeline_pass():
                session_end_reason = "Completed pipeline search/apply pass."
                break
            if not run_non_stop:
                session_end_reason = "Completed one search/apply cycle with continuous mode disabled."
                break
            if cycle_date_posted:
                date_options = ["Any time", "Past month", "Past week", "Past 24 hours"]
                global date_posted
                date_posted = date_options[date_options.index(date_posted)+1 if date_options.index(date_posted)+1 > len(date_options) else -1] if stop_date_cycle_at_24hr else date_options[0 if date_options.index(date_posted)+1 >= len(date_options) else date_options.index(date_posted)+1]
            if alternate_sortby:
                global sort_by
                sort_by = "Most recent" if sort_by == "Most relevant" else "Most relevant"

    except KeyboardInterrupt:
        pipeline_failed = True
        session_end_reason = "Stopped manually by user."
        print_lg(session_end_reason)
    except NoSuchWindowException as e:
        pipeline_failed = True
        session_end_reason = "Browser window closed or session became invalid."
        print_lg(f"{session_end_reason} Ending bot session.", e)
    except WebDriverException as e:
        if is_session_invalid_error(e):
            pipeline_failed = True
            session_end_reason = "Browser window closed or session became invalid."
            print_lg(f"{session_end_reason} Ending bot session.", e)
        else:
            session_end_reason = "Stopped because of a recoverable webdriver issue."
            print_lg(f"{session_end_reason}", e)
    except Exception as e:
        error_message = str(e).strip()
        if is_recoverable_linkedin_login_error(error_message):
            pipeline_failed = True
            session_end_reason = error_message
        else:
            pipeline_failed = True
            session_end_reason = "Stopped because of an unexpected error."
        critical_error_log("In Applier Main", e)
        show_alert(e,alert_title)
    finally:
        summary = "Total runs: {}\nJobs Applied: {}\nExternal job links collected: {}\nTotal processed outcomes: {}\nFailed jobs: {}\nIrrelevant jobs skipped: {}\n".format(total_runs,easy_applied_count,external_jobs_count,easy_applied_count + external_jobs_count,failed_count,skip_count)
        print_lg(summary)
        print_lg("\n\nTotal runs:                     {}".format(total_runs))
        print_lg("Jobs Applied:                   {}".format(easy_applied_count))
        print_lg("External job links collected:   {}".format(external_jobs_count))
        print_lg("                              ----------")
        print_lg("Total processed outcomes:       {}".format(easy_applied_count + external_jobs_count))
        print_lg("\nFailed jobs:                    {}".format(failed_count))
        print_lg("Irrelevant jobs skipped:        {}\n".format(skip_count))
        if randomly_answered_questions: print_lg("\n\nQuestions randomly answered:\n  {}  \n\n".format(";\n".join(str(question) for question in randomly_answered_questions)))
        quotes = choice([
            "Never quit. You're one step closer than before. - Sai Vignesh Golla", 
            "All the best with your future interviews, you've got this. - Sai Vignesh Golla", 
            "Keep up with the progress. You got this. - Sai Vignesh Golla", 
            "If you're tired, learn to take rest but never give up. - Sai Vignesh Golla",
            "Success is not final, failure is not fatal, It is the courage to continue that counts. - Winston Churchill (Not a sponsor)",
            "Believe in yourself and all that you are. Know that there is something inside you that is greater than any obstacle. - Christian D. Larson (Not a sponsor)",
            "Every job is a self-portrait of the person who does it. Autograph your work with excellence. - Jessica Guidobono (Not a sponsor)",
            "The only way to do great work is to love what you do. If you haven't found it yet, keep looking. Don't settle. - Steve Jobs (Not a sponsor)",
            "Opportunities don't happen, you create them. - Chris Grosser (Not a sponsor)",
            "The road to success and the road to failure are almost exactly the same. The difference is perseverance. - Colin R. Davis (Not a sponsor)",
            "Obstacles are those frightful things you see when you take your eyes off your goal. - Henry Ford (Not a sponsor)",
            "The only limit to our realization of tomorrow will be our doubts of today. - Franklin D. Roosevelt (Not a sponsor)",
            ])
        sponsors = "Be the first to have your name here!"
        timeSaved = (easy_applied_count * 80) + (external_jobs_count * 20) + (skip_count * 10)
        timeSavedMsg = ""
        if timeSaved > 0:
            timeSaved += 60
            timeSavedMsg = f"In this run, you saved approx {round(timeSaved/60)} mins ({timeSaved} secs), please consider supporting the project."
        msg = f"{session_end_reason}\n\n{quotes}\n\n\n{timeSavedMsg}\nYou can also get your quote and name shown here, or prioritize your bug reports by supporting the project at:\n\nhttps://github.com/sponsors/GodsScion\n\n\nSummary:\n{summary}\n\n\nBest regards,\nSai Vignesh Golla\nhttps://www.linkedin.com/in/saivigneshgolla/\n\nTop Sponsors:\n{sponsors}"
        show_alert(msg, "Session ended")
        print_lg(msg,"Closing the browser...")
        if tabs_count >= 10:
            msg = "NOTE: IF YOU HAVE MORE THAN 10 TABS OPENED, PLEASE CLOSE OR BOOKMARK THEM!\n\nOr it's highly likely that application will just open browser and not do anything next time!" 
            show_alert(msg,"Info")
            print_lg("\n"+msg)
        ##> ------ Yang Li : MARKYangL - Feature ------
        if use_AI and aiClient:
            try:
                if ai_provider.lower() == "openai":
                    ai_close_openai_client(aiClient)
                elif ai_provider.lower() == "deepseek":
                    ai_close_openai_client(aiClient)
                elif ai_provider.lower() == "gemini":
                    pass # Gemini client does not need to be closed
                print_lg(f"Closed {ai_provider} AI client.")
            except Exception as e:
                print_lg("Failed to close AI client:", e)
        ##<
        try:
            if driver:
                driver.quit()
        except WebDriverException as e:
            print_lg("Browser already closed.", e)
        except Exception as e: 
            critical_error_log("When quitting...", e)

    stage_summary = build_stage_summary(total_runs)
    print_lg(f"LinkedIn stage summary: {stage_summary}")
    if stage_summary["jobs_applied"] > 0 and stage_summary["rows_written_to_applied_csv"] == 0:
        pipeline_failed = True
        session_end_reason = "Confirmed applications were submitted but none were written to applied_jobs.csv."
        print_lg(session_end_reason)
    exit_code = 1 if pipeline_failed else 0
    return {
        "exit_code": exit_code,
        "session_end_reason": session_end_reason,
        "unexpected_failure": pipeline_failed,
        **stage_summary,
    }


if __name__ == "__main__":
    raise SystemExit(main()["exit_code"])






