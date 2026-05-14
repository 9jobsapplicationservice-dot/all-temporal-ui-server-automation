'''
Author:     Sai Vignesh Golla
LinkedIn:   https://www.linkedin.com/in/saivigneshgolla/

Copyright (C) 2024 Sai Vignesh Golla

License:    GNU Affero General Public License
            https://www.gnu.org/licenses/agpl-3.0.en.html
            
GitHub:     https://github.com/GodsScion/Auto_job_applier_linkedIn

Support me: https://github.com/sponsors/GodsScion

version:    24.12.3.10.30
'''

import os


def _read_bool_env(name: str, default: bool) -> bool:
    value = os.environ.get(name, "").strip().lower()
    if not value:
        return default
    return value in {"1", "true", "yes", "on"}

def _read_str_env(name: str, default: str) -> str:
    value = os.environ.get(name, "").strip()
    return value if value else default


###################################################### CONFIGURE YOUR TOOLS HERE ######################################################


# Login Credentials for LinkedIn (Optional)
username = 'simrankaur9jobs@gmail.com'
password = 'Alok@123'
linkedin_auto_login = True
target_job_link = ''


## Artificial Intelligence (Beta Not-Recommended)
# Use AI
use_AI = True
'''
Note: Set it as True only if you want to use AI, and If you either have a
1. Local LLM model running on your local machine, with it's APIs exposed. Example softwares to achieve it are:
    a. Ollama - https://ollama.com/
    b. llama.cpp - https://github.com/ggerganov/llama.cpp
    c. LM Studio - https://lmstudio.ai/ (Recommended)
    d. Jan - https://jan.ai/
2. OR you have a valid OpenAI API Key, and money to spare, and you don't mind spending it.
CHECK THE OPENAI API PIRCES AT THEIR WEBSITE (https://openai.com/api/pricing/). 
'''

##> ------ Yang Li : MARKYangL - Feature ------
##> ------ Tim L : tulxoro - Refactor ------
# Select AI Provider
ai_provider = 'gemini'
'''
Note: Select your AI provider.
* "openai" - OpenAI API (GPT models) OR OpenAi-compatible APIs (like Ollama)
* "deepseek" - DeepSeek API (DeepSeek models)
* "gemini" - Google Gemini API (Gemini models)
* For any other models, keep it as "openai" if it is compatible with OpenAI's api.
'''



# Your LLM url or other AI api url and port
llm_api_url = 'https://generativelanguage.googleapis.com/v1beta/'
'''
Note: Don't forget to add / at the end of your url. You may not need this if you are using Gemini.
'''

# Your LLM API key or other AI API key 
llm_api_key = ''
'''
Note: Leave it empty as "" or "not-needed" if not needed. Else will result in error!
If you are using ollama, you MUST put "not-needed".
'''

# Your LLM model name or other AI model name
llm_model = 'gemini-1.5-flash'

llm_spec = 'gemini'
'''
Note: Currently "openai", "deepseek", "gemini" and "openai-like" api endpoints are supported.
Most LLMs are compatible with openai, so keeping it as "openai-like" will work.
'''

# # Yor local embedding model name or other AI Embedding model name
# llm_embedding_model = "nomic-embed-text-v1.5"

# Do you want to stream AI output?
stream_output = False
'''
Set `stream_output = True` if you want to stream AI output or `stream_output = False` if not.
'''
##




############################################################################################################
'''
THANK YOU for using my tool 😊! Wishing you the best in your job hunt 🙌🏻!

Sharing is caring! If you found this tool helpful, please share it with your peers 🥺. Your support keeps this project alive.

Support my work on <PATREON_LINK>. Together, we can help more job seekers.

As an independent developer, I pour my heart and soul into creating tools like this, driven by the genuine desire to make a positive impact.

Your support, whether through donations big or small or simply spreading the word, means the world to me and helps keep this project alive and thriving.

Gratefully yours 🙏🏻,
Sai Vignesh Golla
'''
############################################################################################################
