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
import sys
import json
import pathlib
import time

from time import sleep
from random import randint
from datetime import datetime, timedelta
from pprint import pformat

from config.settings import logs_folder_path



#### Common functions ####

#< Directories related
def make_directories(paths: list[str]) -> None:
    '''
    Function to create missing directories
    '''
    for path in paths:
        path = os.path.expanduser(path) # Expands ~ to user's home directory
        path = path.replace("//","/")
        
        # If path looks like a file path, get the directory part
        if '.' in os.path.basename(path):
            path = os.path.dirname(path)

        if not path: # Handle cases where path is empty after dirname
            continue

        try:
            if not os.path.exists(path):
                os.makedirs(path, exist_ok=True) # exist_ok=True avoids race condition
        except Exception as e:
            print(f'Error while creating directory "{path}": ', e)


def get_default_temp_profile() -> str:
    # Use PIPELINE_RUN_ID to create a unique profile for each run to support multiple users
    run_id = os.environ.get("PIPELINE_RUN_ID", "default")
    home = pathlib.Path.home()
    if sys.platform.startswith('win'):
        return f"C:\\temp\\auto-job-apply-profile-{run_id}"
    elif sys.platform.startswith('linux'):
        # On Linux/Render, use /tmp for faster access and to avoid disk quota issues
        return f"/tmp/auto-job-apply-profile-{run_id}"
    return str(home / "Library" / "Application Support" / "Google" / "Chrome" / f"auto-job-apply-profile-{run_id}")


def find_default_profile_directory() -> str | None:
    '''
    Dynamically finds the default Google Chrome 'User Data' directory path
    across Windows, macOS, and Linux, regardless of OS version.

    Returns the absolute path as a string, or None if the path is not found.
    '''
    
    home = pathlib.Path.home()
    
    # Windows
    if sys.platform.startswith('win'):
        paths = [
            os.path.expandvars(r"%LOCALAPPDATA%\Google\Chrome\User Data"),
            os.path.expandvars(r"%USERPROFILE%\AppData\Local\Google\Chrome\User Data"),
            os.path.expandvars(r"%USERPROFILE%\Local Settings\Application Data\Google\Chrome\User Data")
        ]
    # Linux
    elif sys.platform.startswith('linux'):
        paths = [
            str(home / ".config" / "google-chrome"),
            str(home / ".var" / "app" / "com.google.Chrome" / "data" / ".config" / "google-chrome"),
        ]
    # MacOS ## For some reason, opening with profile in MacOS is not creating a session for undetected-chromedriver!
    # elif sys.platform == 'darwin':
    #     paths = [
    #         str(home / "Library" / "Application Support" / "Google" / "Chrome")
    #     ]
    else:
        return None

    # Check each potential path and return the first one that exists
    for path_str in paths:
        if os.path.exists(path_str):
            return path_str
            
    return None
#>


#< Logging related
_TRUTHY_ENV_VALUES = {"1", "true", "yes", "on"}
PIPELINE_MODE = os.environ.get("PIPELINE_MODE", "").strip().lower() in _TRUTHY_ENV_VALUES
PIPELINE_ENABLE_POPUPS = os.environ.get("PIPELINE_ENABLE_POPUPS", "").strip().lower() in _TRUTHY_ENV_VALUES
__log_file_warning_shown = False


def is_pipeline_mode() -> bool:
    return PIPELINE_MODE


def is_pipeline_popup_mode() -> bool:
    return PIPELINE_MODE and PIPELINE_ENABLE_POPUPS


def _stringify_message(value) -> str:
    if isinstance(value, str):
        return value
    try:
        return str(value)
    except Exception:
        return repr(value)


def _normalize_buttons(buttons) -> list[str]:
    if buttons is None:
        return ["OK"]
    if isinstance(buttons, (list, tuple)):
        normalized = [_stringify_message(button) for button in buttons if _stringify_message(button)]
        return normalized or ["OK"]
    normalized = _stringify_message(buttons)
    return [normalized] if normalized else ["OK"]


def _pick_confirm_fallback(buttons: list[str]) -> str:
    priorities = (
        "continue",
        "look's good",
        "looks good",
        "disable pause",
        "skip confirmation",
        "no",
        "ok",
        "okay",
        "yes",
    )
    lowered_buttons = [(button.lower(), button) for button in buttons]
    for priority in priorities:
        for lowered, original in lowered_buttons:
            if priority in lowered:
                return original
    return buttons[-1] if buttons else "OK"


def _emit_console_line(text: str, end: str = "\n", flush: bool = False) -> None:
    full_text = _stringify_message(text) + end
    stream = getattr(sys, 'stdout', None) or getattr(sys, '__stdout__', None)
    if stream is None:
        return

    try:
        stream.write(full_text)
        if flush:
            stream.flush()
        return
    except UnicodeEncodeError:
        encoding = getattr(stream, 'encoding', None) or 'utf-8'
        safe_bytes = full_text.encode(encoding, errors='backslashreplace')
        if hasattr(stream, 'buffer'):
            stream.buffer.write(safe_bytes)
            if flush:
                stream.buffer.flush()
            return
        fallback_text = safe_bytes.decode(encoding, errors='ignore')
    except Exception:
        fallback_text = full_text.encode('ascii', errors='backslashreplace').decode('ascii')

    fallback_stream = getattr(sys, '__stdout__', None) or stream
    try:
        fallback_stream.write(fallback_text)
        if flush:
            fallback_stream.flush()
    except Exception:
        pass


def show_alert(message=None, title="Info", button="OK", text=None):
    alert_message = _stringify_message(text if text is not None else message)
    alert_title = _stringify_message(title)
    alert_button = _stringify_message(button or "OK")

    if PIPELINE_MODE:
        _emit_console_line(f"[PIPELINE ALERT SUPPRESSED] {alert_title}: {alert_message}")
        return alert_button

    try:
        import pyautogui
        return pyautogui.alert(alert_message, alert_title, alert_button)
    except Exception as error:
        _emit_console_line(f"[ALERT FALLBACK] {alert_title}: {alert_message} | {error}")
        return alert_button


def show_confirm(message=None, title="Info", buttons=None, text=None):
    confirm_message = _stringify_message(text if text is not None else message)
    confirm_title = _stringify_message(title)
    confirm_buttons = _normalize_buttons(buttons)

    if PIPELINE_MODE:
        fallback_button = _pick_confirm_fallback(confirm_buttons)
        _emit_console_line(f"[PIPELINE CONFIRM SUPPRESSED] {confirm_title}: {confirm_message} -> {fallback_button}")
        return fallback_button

    try:
        import pyautogui
        return pyautogui.confirm(confirm_message, confirm_title, confirm_buttons)
    except Exception as error:
        fallback_button = _pick_confirm_fallback(confirm_buttons)
        _emit_console_line(f"[CONFIRM FALLBACK] {confirm_title}: {confirm_message} | {error} -> {fallback_button}")
        return fallback_button


def critical_error_log(possible_reason: str, stack_trace: Exception) -> None:
    '''
    Function to log and print critical errors along with datetime stamp
    '''
    print_lg(possible_reason, stack_trace, datetime.now(), from_critical=True)


def get_log_path() -> str:
    '''
    Function to replace '//' with '/' for logs path
    '''
    try:
        path = logs_folder_path + "/log.txt"
        return path.replace("//", "/")
    except Exception:
        return "logs/log.txt"


__logs_file_path = get_log_path()


def _ensure_log_parent() -> None:
    make_directories([__logs_file_path])


def _is_log_lock_error(error: Exception) -> bool:
    if isinstance(error, PermissionError):
        return True
    if isinstance(error, OSError) and getattr(error, 'winerror', None) in {5, 32, 33}:
        return True
    return False


def _warn_log_file_locked_once(error: Exception, message_text: str, from_critical: bool) -> None:
    global __log_file_warning_shown
    trail = f'Skipped saving this message: "{message_text}" to log.txt!' if from_critical else "Continuing without file logging for this message."
    warning = f"log.txt in {logs_folder_path} appears locked or unavailable. {trail}"
    if not __log_file_warning_shown:
        __log_file_warning_shown = True
        show_alert(warning, "Failed Logging")
    _emit_console_line(f"[LOG FILE WARNING] {warning} | {error}")


def print_lg(*msgs: str | dict, end: str = "\n", pretty: bool = False, flush: bool = False, from_critical: bool = False) -> None:
    '''
    Function to log and print. **Note that, `end` and `flush` parameters are ignored if `pretty = True`**
    '''
    message_end = "\n" if pretty else end
    should_flush = False if pretty else flush

    for raw_message in msgs:
        message_text = pformat(raw_message) if pretty else _stringify_message(raw_message)
        _emit_console_line(message_text, end=message_end, flush=should_flush or PIPELINE_MODE)

        try:
            _ensure_log_parent()
            with open(__logs_file_path, 'a+', encoding='utf-8') as file:
                file.write(message_text + message_end)
                if PIPELINE_MODE:
                    file.flush()
        except Exception as error:
            if _is_log_lock_error(error):
                _warn_log_file_locked_once(error, message_text, from_critical)
            else:
                _emit_console_line(f"[LOGGING WARNING] Failed writing to {__logs_file_path}: {error}")
#>


def buffer(speed: int=0) -> None:
    '''
    Function to wait within a period of selected random range.
    * Will not wait if input `speed <= 0`
    * Will wait within a random range of 
      - `0.6 to 1.0 secs` if `1 <= speed < 2`
      - `1.0 to 1.8 secs` if `2 <= speed < 3`
      - `1.8 to speed secs` if `3 <= speed`
    '''
    if speed<=0:
        return
    elif speed <= 1 and speed < 2:
        return sleep(randint(6,10)*0.1)
    elif speed <= 2 and speed < 3:
        return sleep(randint(10,18)*0.1)
    else:
        return sleep(randint(18,round(speed)*10)*0.1)
    

def manual_login_retry(is_logged_in: callable, limit: int = 2) -> bool:
    '''
    Function to ask and validate manual login
    '''
    if PIPELINE_MODE and not PIPELINE_ENABLE_POPUPS:
        timeout_seconds = int(os.environ.get("PIPELINE_MANUAL_LOGIN_TIMEOUT_SECONDS", "180") or "180")
        deadline = time.time() + max(timeout_seconds, 1)
        print_lg(
            "Pipeline mode detected. Waiting for manual LinkedIn login in the opened Chrome window. "
            f"The bot will keep checking for up to {timeout_seconds} seconds."
        )
        while time.time() < deadline:
            if is_logged_in():
                print_lg("Manual LinkedIn login detected. Continuing automation.")
                return True
            sleep(2)
        print_lg("Timed out while waiting for manual LinkedIn login confirmation in pipeline mode.")
        return is_logged_in()

    count = 0
    while not is_logged_in():
        print_lg("Seems like you're not logged in!")
        button = "Confirm Login"
        message = 'After you successfully Log In, please click "{}" button below.'.format(button)
        if count > limit:
            button = "Skip Confirmation"
            message = 'If you\'re seeing this message even after you logged in, Click "{}". Seems like auto login confirmation failed!'.format(button)
        count += 1
        if show_alert(message, "Login Required", button) and count > limit:
            return is_logged_in()
    return True



def calculate_date_posted(time_string: str) -> datetime | None | ValueError:
    '''
    Function to calculate date posted from string.
    Returns datetime object | None if unable to calculate | ValueError if time_string is invalid
    Valid time string examples:
    * 10 seconds ago
    * 15 minutes ago
    * 2 hours ago
    * 1 hour ago
    * 1 day ago
    * 10 days ago
    * 1 week ago
    * 1 month ago
    * 1 year ago
    '''
    import re
    time_string = time_string.strip()
    now = datetime.now()

    match = re.search(r'(\d+)\s+(second|minute|hour|day|week|month|year)s?\s+ago', time_string, re.IGNORECASE)

    if match:
        try:
            value = int(match.group(1))
            unit = match.group(2).lower()

            if 'second' in unit:
                return now - timedelta(seconds=value)
            elif 'minute' in unit:
                return now - timedelta(minutes=value)
            elif 'hour' in unit:
                return now - timedelta(hours=value)
            elif 'day' in unit:
                return now - timedelta(days=value)
            elif 'week' in unit:
                return now - timedelta(weeks=value)
            elif 'month' in unit:
                return now - timedelta(days=value * 30)  # Approximation
            elif 'year' in unit:
                return now - timedelta(days=value * 365)  # Approximation
        except (ValueError, IndexError):
            # Fallback for cases where parsing fails
            pass
    
    # If regex doesn't match, or parsing failed, return None.
    # This will skip jobs where the date can't be determined, preventing crashes.
    return None


def convert_to_lakhs(value: str) -> str:
    '''
    Converts str value to lakhs, no validations are done except for length and stripping.
    Examples:
    * "100000" -> "1.00"
    * "101,000" -> "10.1," Notice ',' is not removed 
    * "50" -> "0.00"
    * "5000" -> "0.05" 
    '''
    value = value.strip()
    l = len(value)
    if l > 0:
        if l > 5:
            value = value[:l-5] + "." + value[l-5:l-3]
        else:
            value = "0." + "0"*(5-l) + value[:2]
    return value


def convert_to_json(data) -> dict:
    '''
    Function to convert data to JSON, if unsuccessful, returns `{"error": "Unable to parse the response as JSON", "data": data}`
    '''
    try:
        result_json = json.loads(data)
        return result_json
    except json.JSONDecodeError:
        return {"error": "Unable to parse the response as JSON", "data": data}


def truncate_for_csv(data, max_length: int = 131000, suffix: str = "...[TRUNCATED]") -> str:
    '''
    Function to truncate data for CSV writing to avoid field size limit errors.
    * Takes in `data` of any type and converts to string
    * Takes in `max_length` of type `int` - maximum allowed length (default: 131000, leaving room for suffix)
    * Takes in `suffix` of type `str` - text to append when truncated
    * Returns truncated string if data exceeds max_length
    '''
    try:
        # Convert data to string
        str_data = str(data) if data is not None else ""
        
        # If within limit, return as-is
        if len(str_data) <= max_length:
            return str_data
        
        # Truncate and add suffix
        truncated = str_data[:max_length - len(suffix)] + suffix
        return truncated
    except Exception as e:
        return f"[ERROR CONVERTING DATA: {e}]"
