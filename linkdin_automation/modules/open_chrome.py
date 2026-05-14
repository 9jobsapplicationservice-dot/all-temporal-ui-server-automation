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

import os
import re
import subprocess

from modules.helpers import get_default_temp_profile, make_directories
from config.settings import run_in_background, stealth_mode, disable_extensions, safe_mode, file_name, failed_file_name, logs_folder_path, screenshot_folder_path, generated_resume_path
from config.questions import default_resume_path
from config.secrets import linkedin_auto_login, target_job_link
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from modules.helpers import find_default_profile_directory, critical_error_log, print_lg, show_alert
from selenium.common.exceptions import SessionNotCreatedException


def _parse_major_version(version_text):
    if not version_text:
        return None
    match = re.search(r"(\d+)\.", version_text.strip())
    return int(match.group(1)) if match else None


def _run_command_for_version(command):
    try:
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=False,
            timeout=5,
            shell=False,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    output = (result.stdout or result.stderr or "").strip()
    return _parse_major_version(output)


def _detect_windows_chrome_major_version():
    env_override = os.getenv("CHROME_VERSION_MAIN")
    if env_override:
        override_version = _parse_major_version(env_override)
        if override_version is not None:
            print_lg(f"Using Chrome major version override from CHROME_VERSION_MAIN={override_version}")
            return override_version
        print_lg(f"Ignoring invalid CHROME_VERSION_MAIN value: {env_override}")

    commands_to_try = [
        [
            "reg",
            "query",
            r"HKEY_CURRENT_USER\Software\Google\Chrome\BLBeacon",
            "/v",
            "version",
        ],
        [
            "reg",
            "query",
            r"HKEY_LOCAL_MACHINE\Software\Google\Chrome\BLBeacon",
            "/v",
            "version",
        ],
        [
            "powershell",
            "-Command",
            "(Get-Item 'C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe').VersionInfo.ProductVersion",
        ],
        [
            "powershell",
            "-Command",
            "(Get-Item 'C:\\Program Files (x86)\\Google\\Chrome\\Application\\chrome.exe').VersionInfo.ProductVersion",
        ],
        [
            "powershell",
            "-Command",
            "(Get-Item \"$env:LOCALAPPDATA\\Google\\Chrome\\Application\\chrome.exe\").VersionInfo.ProductVersion",
        ],
    ]

    for command in commands_to_try:
        version_main = _run_command_for_version(command)
        if version_main is not None:
            print_lg(f"Detected Chrome major version {version_main} for undetected-chromedriver")
            return version_main

    print_lg("Could not detect Chrome major version; starting undetected-chromedriver without version pin")
    return None


def _create_undetected_chrome(options):
    try:
        import undetected_chromedriver as uc
    except Exception as error:
        raise RuntimeError("undetected_chromedriver is unavailable for this Python environment.") from error
    print_lg("Downloading Chrome Driver... This may take some time. Undetected mode requires download every run!")
    version_main = _detect_windows_chrome_major_version()
    if version_main is None:
        return uc.Chrome(options=options)
    return uc.Chrome(options=options, version_main=version_main)


def _should_prefer_stable_chrome() -> bool:
    return bool(target_job_link.strip()) and not linkedin_auto_login and not run_in_background


def createChromeSession(isRetry: bool = False, force_stable: bool = False):
    make_directories([file_name,failed_file_name,screenshot_folder_path,default_resume_path,generated_resume_path+"/temp"])
    use_stealth_mode = stealth_mode and not force_stable
    # Set up WebDriver with Chrome Profile
    if use_stealth_mode:
        try:
            import undetected_chromedriver as uc
            options = uc.ChromeOptions()
        except Exception as error:
            raise RuntimeError("undetected_chromedriver is unavailable for this Python environment.") from error
    else:
        options = Options()
    if os.name == 'posix':
        # Standard paths for Chromium on Render/Linux
        for path in ['/usr/bin/chromium', '/usr/bin/chromium-browser', '/usr/bin/google-chrome']:
            if os.path.exists(path):
                options.binary_location = path
                break
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-gpu')

    if run_in_background:   options.add_argument("--headless")
    if disable_extensions:  options.add_argument("--disable-extensions")

    print_lg("IF YOU HAVE MORE THAN 10 TABS OPENED, PLEASE CLOSE OR BOOKMARK THEM! Or it's highly likely that application will just open browser and not do anything!")
    profile_dir = find_default_profile_directory()
    if isRetry:
        print_lg("Will login with a guest profile, browsing history will not be saved in the browser!")
    elif profile_dir and not safe_mode:
        options.add_argument(f"--user-data-dir={profile_dir}")
    else:
        print_lg("Logging in with a guest profile, Web history will not be saved!")
        options.add_argument(f"--user-data-dir={get_default_temp_profile()}")
    if use_stealth_mode:
        driver = _create_undetected_chrome(options)
    else:
        driver = webdriver.Chrome(options=options)
    driver.maximize_window()
    wait = WebDriverWait(driver, 5)
    actions = ActionChains(driver)
    return options, driver, actions, wait

options, driver, actions, wait = None, None, None, None


def initializeChromeSession():
    global options, driver, actions, wait

    if driver is not None:
        return options, driver, actions, wait

    prefer_stable = _should_prefer_stable_chrome()
    attempt_order = (
        [(False, True), (True, True), (False, False), (True, False)]
        if prefer_stable else
        [(False, False), (True, False), (False, True), (True, True)]
    )

    last_error = None
    for is_retry, force_stable in attempt_order:
        try:
            mode_name = "stable Selenium Chrome" if force_stable else "undetected Chrome"
            profile_name = "guest profile" if is_retry else "default profile"
            print_lg(f"Launching Chrome using {mode_name} with {profile_name}.")
            options, driver, actions, wait = createChromeSession(is_retry, force_stable=force_stable)
            return options, driver, actions, wait
        except SessionNotCreatedException as error:
            last_error = error
            critical_error_log("Failed to create Chrome Session, trying next Chrome startup fallback", error)
            try:
                if driver:
                    driver.quit()
            except Exception:
                pass
            options, driver, actions, wait = None, None, None, None
        except Exception as error:
            last_error = error
            critical_error_log("Chrome launch attempt failed, trying next Chrome startup fallback", error)
            try:
                if driver:
                    driver.quit()
            except Exception:
                pass
            options, driver, actions, wait = None, None, None, None

    msg = 'Failed to open Chrome reliably. Try closing old Chrome windows, keep safe_mode = True, and use stealth_mode = False for manual login runs.'
    if isinstance(last_error, TimeoutError):
        msg = "Couldn't download Chrome-driver. Keep stealth_mode = False for this manual-login flow."
    print_lg(msg)
    if last_error is not None:
        critical_error_log("In Opening Chrome", last_error)
    show_alert(msg, "Error in opening chrome")
    raise RuntimeError(msg) from last_error

    return options, driver, actions, wait
