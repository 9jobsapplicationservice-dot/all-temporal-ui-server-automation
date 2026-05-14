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


def _read_bool_env(name: str, default: bool) -> bool:
    value = os.environ.get(name, '').strip().lower()
    if not value:
        return default
    return value in {'1', 'true', 'yes', 'on'}


def _read_path_env(name: str, default: str) -> str:
    value = os.environ.get(name, '').strip()
    return value if value else default


def _read_int_env(name: str, default: int) -> int:
    value = os.environ.get(name, '').strip()
    if not value:
        return default
    try:
        return int(value)
    except ValueError:
        return default

###################################################### CONFIGURE YOUR BOT HERE ######################################################

# >>>>>>>>>>> LinkedIn Settings <<<<<<<<<<<

# Keep the External Application tabs open?
close_tabs = False
'''
Note: RECOMMENDED TO LEAVE IT AS `True`, if you set it `False`, be sure to CLOSE ALL TABS BEFORE CLOSING THE BROWSER!!!
'''

# Follow easy applied companies
follow_companies = False

## Upcoming features (In Development)
# # Send connection requests to HR's 
# connect_hr = True                  # True or False, Note: True or False are case-sensitive

# # What message do you want to send during connection request? (Max. 200 Characters)
# connect_request_message = ""       # Leave Empty to send connection request without personalized invitation (recommended to leave it empty, since you only get 10 per month without LinkedIn Premium*)

# Do you want the program to keep running until you stop it? (Recommended)
run_non_stop = True
'''
Note: Keeps searching in repeated cycles until you manually stop it or a hard stop condition occurs.
Will be treated as False if `run_in_background = True`
'''
alternate_sortby = True
cycle_date_posted = True
stop_date_cycle_at_24hr = True
pipeline_max_easy_apply = 50





# >>>>>>>>>>> RESUME GENERATOR (Experimental & In Development) <<<<<<<<<<<

# Give the path to the folder where all the generated resumes are to be stored
generated_resume_path = 'all resumes/'





# >>>>>>>>>>> Global Settings <<<<<<<<<<<

# Directory and name of the files where history of applied jobs is saved (Sentence after the last "/" will be considered as the file name).
file_name = 'all excels/all_applied_applications_history.csv'
recruiters_file_name = 'all excels/recruiters_enriched.csv'
failed_file_name = 'all excels/all_failed_applications_history.csv'
external_jobs_file_name = 'all excels/external_jobs.csv'
logs_folder_path = 'logs/'
screenshot_folder_path = ''

# Set the maximum amount of time allowed to wait between each click in secs
click_gap = 1

# Pause and wait for you when Easy Apply shows a custom form page that needs manual input.
manual_pause_on_form = False

# Additional pacing for Easy Apply steps to avoid very fast button clicks. 0 = off, 1 = light, 2 = moderate, 3 = slower
manual_form_pacing = 2

# If you want to see Chrome running then set run_in_background as False (May reduce performance).
run_in_background = False

# If you want to disable extensions then set disable_extensions as True (Better for performance)
disable_extensions = False

# Run in safe mode. Set this true if chrome is taking too long to open or if you have multiple profiles in browser. This will open chrome in guest profile!
safe_mode = False                   # True or False, Note: True or False are case-sensitive
safe_mode = False

# Do you want scrolling to be smooth or instantaneous? (Can reduce performance if True)
smooth_scroll = False

# If enabled (True), the program would keep your screen active and prevent PC from sleeping. Instead you could disable this feature (set it to false) and adjust your PC sleep settings to Never Sleep or a preferred time. 
keep_screen_awake = True

# Run in undetected mode to bypass anti-bot protections (Preview Feature, UNSTABLE. Recommended to leave it as False)
stealth_mode = False

# Do you want to get alerts on errors related to AI API connection?
showAiErrorAlerts = False

# Use ChatGPT for resume building (Experimental Feature can break the application. Recommended to leave it as False) 
# use_resume_generator = False       # True or False, Note: True or False are case-sensitive ,   This feature may only work with 'stealth_mode = True'. As ChatGPT website is hosted by CloudFlare which is protected by Anti-bot protections!











############################################################################################################
'''
THANK YOU for using my tool ðŸ˜Š! Wishing you the best in your job hunt ðŸ™ŒðŸ»!

Sharing is caring! If you found this tool helpful, please share it with your peers ðŸ¥º. Your support keeps this project alive.

Support my work on <PATREON_LINK>. Together, we can help more job seekers.

As an independent developer, I pour my heart and soul into creating tools like this, driven by the genuine desire to make a positive impact.

Your support, whether through donations big or small or simply spreading the word, means the world to me and helps keep this project alive and thriving.

Gratefully yours ðŸ™ðŸ»,
Sai Vignesh Golla
'''
############################################################################################################


