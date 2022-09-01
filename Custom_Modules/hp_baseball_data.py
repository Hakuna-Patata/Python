"""
This module is used to pull baseball data
"""

import os as _os
import time as _time
import urllib.parse as _urlparse
import requests as _requests
import io as _io
import pandas as _pd
from bs4 import BeautifulSoup as _BS
from zipfile import ZipFile as _ZF


def check_packages():
    try:
        from selenium import webdriver as _webdriver
        options = _webdriver.ChromeOptions()
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        wd = _webdriver.Chrome('chromedriver', options=options)
        del wd 
    except:
        try:
            def install_packages():
                _os.system('apt-get update')
                _os.system('apt install chromium-chromedriver')
                _os.system('pip install selenium')

            install_packages()
        except:
            print('Error: Package install failure!')

batting_stats = r"c,3,6,5,8,9,10,11,13,12,14,17,21,39,51,61,316,50,317,102,103,107,206,207,208,211,308,305,306,311,60"

batting_params_dict = {
    'lg': 'all'  # league (all, al, nl)
    , 'month': '0'  # 0-full stats, 1-past 7 days, 2-past 14 days, 3-past 30 days
    , 'pos': 'all'  # position (ex. 2b, 1b, rf, of, c, p)
    , 'qual': '0'  # min plate appearances
    , 'season': '2022'  # season year
    , 'stats': 'bat'  #bat=batting, pit=pitching, fld=fielding
    , 'type': batting_stats  # custom metrics
    , 'startdate': ''  # format yyyy-mm-dd
    , 'enddate': ''  #format yyyy-mm-dd 
    , 'sort': '15,d'  # format (column_number, [d|a])
 }

def fan_graph_leaderboard_data(base_url=r"https://www.fangraphs.com/leaders.aspx", new_file_name=None, decoded_params_dict=batting_params_dict, dwnld_path='/content/fg_data', dwnld_element_id='LeaderBoard1_cmdCSV', timeout=30):

    check_packages()  # check if proper packages installed and if not, install them
    from selenium import webdriver as _webdriver

    ## create options for web driver that will allow it to run properly
    options = _webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')

    ## set download path for web driver
    if dwnld_path is not None:
        if not _os.path.exists(dwnld_path):
            _os.mkdir(dwnld_path)
            prefs = {'download.default_directory':dwnld_path}
            options.add_experimental_option('prefs', prefs)
        else:
            pass
    else:
        pass 

    ## create Chrome web driver
    wd = _webdriver.Chrome('chromedriver', options=options)

    ## url encode parameters dictionary
    encoded_params = _urlparse.urlencode(batting_params_dict)

    parsed_url = _urlparse.urlparse(base_url)  # parse base url into request parts
    new_parsed_url = parsed_url._replace(query=encoded_params)  # replace query part of parsed url with encoded dictionary parameters
    new_url = _urlparse.urlunparse(new_parsed_url)  # unparse to get the new url

    wd.get(new_url)  # GET request to new url 
    download_element = wd.execute_script(f'return document.getElementById("{dwnld_element_id}");')  # return download element
    wd.execute_script("arguments[0].click();", download_element)  # click the download element

    time_max_sec = timeout
    timeout_seconds = list(range(1,time_max_sec+1))

    _os.chdir(dwnld_path)
    for sec in timeout_seconds:
        file_name = 'FanGraphs Leaderboard.csv'
        if _os.path.exists(file_name):
            if new_file_name is None:
                continue 
            else:
                file_name = new_file_name
            _os.rename('FanGraphs Leaderboard.csv', file_name)
            print(f"{file_name} successfully downloaded!!")
            break
        else:
            if sec == time_max_sec:
                print("ERROR: Download stopped! Timeout limit reached.")
            else:
                _time.sleep(1)


def update_url_params(base_url, new_params):
   
    encoded_params = _urlparse.urlencode(new_params)
   
    parsed_url = _urlparse.urlparse(base_url)  # parse base url into request parts
    new_parsed_url = parsed_url._replace(query=encoded_params)  # replace query part of parsed url with encoded dictionary parameters
    new_url = _urlparse.urlunparse(new_parsed_url)  # unparse to get the new url

    return new_url


def retro_sheet_data(base_url=r"https://www.retrosheet.org/gamelogs/index.html", year='2022', dwnld_path=r"/content/retrosheet_data"):
    base_url = r'https://www.retrosheet.org/gamelogs/index.html'

    s = _requests.Session()
    r = s.get(base_url, timeout=10)

    soup = _BS(r.text, 'lxml')

    for a in soup.find_all("a"):
        try:
            a_href = a["href"]
            a_text = a.text
        except:
            a_href = None
            a_text = None

        if a_text == "2021":
            get = _requests.get(a_href)
            zf = _ZF(_io.BytesIO(get.content))
            zf.extractall(dwnld_path)

        else:
            continue 

retro_game_log_cols = ["DT"
    , "N_GAMES"
    , "DOW"
    , "VISITING_TEAM"
    , "VISITING_LEAGUE"
    , "VISITING_TEAM_GAME_NUM"
    , "HOME_TEAM"
    , "HOME_LEAGUE"
    , "HOME_TEAM_GAME_NUM"
    , "VISITING_SCORE"
    , "HOME_SCORE"
    , "LEN_GAME_OUTS"
    , "DAY/NIGHT"
    , "COMPLT_INFO" # if game suspended, postponed, etc., will show the completion info in the format "yyyymmdd,park,vs,hs,len"
    , "FORFEIT_INFO"  # V[visiting forfeit], H[home forfeit], T[no decision]
    , "PROTEST_INFO"  # P[game protested by unidentified team], V[disallowed protest was made by visiting], H[disallowed protest made by home], X[upheld protest by visiting], Y[upheld protest by home]
    , "PARK_ID"
    , "ATTENDANCE"
    , "GAME_TIME_MIN"
    , "VISITING_LINE_SCORE"
    , "HOME_LINE_SCORE"
    , "VISITING_OFF_AT_BATS"
    , "VISITING_OFF_HITS"
    , "VISITING_OFF_DOUBLES"
    , "VISITING_OFF_TRIPLES"
    , "VISITING_OFF_HR"
    , "VISITING_OFF_RBI"
    , "VISITING_OFF_SAC_HITS"
    , "VISITING_OFF_SAC_FLY"
    , "VISITING_OFF_HIT_BY_PITCH"
    , "VISITING_OFF_WALK"
    , "VISITING_OFF_INT_WALK"
    , "VISITING_OFF_STRUCK_OUT"
    , "VISITING_OFF_STOLEN_BASE"
    , "VISITING_OFF_CAUGHT_STEALING"
    , "VISITING_OFF_GROUND_INTO_DOUBLE_PLAY"
    , "VISITING_OFF_CATCHER_INTERFERENCE"
    , "VISITING_OFF_LEFT_ON_BASE"
    , "VISITING_PIT_PITCHERS_USED"
    , "VISITING_PIT_EARNED_RUNS(INDIVIDUAL)"
    , "VISITING_PIT_EARNED_RUNS(TEAM)"
    , "VISITING_PIT_WILD_PITCHES"
    , "VISITING_PIT_BALKS"
    , "VISITING_DEF_PUTOUTS"
    , "VISITING_DEF_ASSISTS"
    , "VISITING_DEF_ERRORS"
    , "VISITING_DEF_PASSED_BALLS"
    , "VISITING_DEF_DOUBLE_PLAYS"
    , "VISITING_DEF_TRIPLE_PLAYS"
    , "HOME_OFF_AT_BATS"
    , "HOME_OFF_HITS"
    , "HOME_OFF_DOUBLES"
    , "HOME_OFF_TRIPLES"
    , "HOME_OFF_HR"
    , "HOME_OFF_RBI"
    , "HOME_OFF_SAC_HITS"
    , "HOME_OFF_SAC_FLY"
    , "HOME_OFF_HIT_BY_PITCH"
    , "HOME_OFF_WALK"
    , "HOME_OFF_INT_WALK"
    , "HOME_OFF_STRUCK_OUT"
    , "HOME_OFF_STOLEN_BASE"
    , "HOME_OFF_CAUGHT_STEALING"
    , "HOME_OFF_GROUND_INTO_DOUBLE_PLAY"
    , "HOME_OFF_CATCHER_INTERFERENCE"
    , "HOME_OFF_LEFT_ON_BASE"
    , "HOME_PIT_PITCHERS_USED"
    , "HOME_PIT_EARNED_RUNS(INDIVIDUAL)"
    , "HOME_PIT_EARNED_RUNS(TEAM)"
    , "HOME_PIT_WILD_PITCHES"
    , "HOME_PIT_BALKS"
    , "HOME_DEF_PUTOUTS"
    , "HOME_DEF_ASSISTS"
    , "HOME_DEF_ERRORS"
    , "HOME_DEF_PASSED_BALLS"
    , "HOME_DEF_DOUBLE_PLAYS"
    , "HOME_DEF_TRIPLE_PLAYS"
    , "HOME_PLATE_UMPIRE_ID"
    , "HOME_PLATE_UMPIRE_NAME"
    , "1B_UMPIRE_ID"
    , "1B_UMPIRE_NAME"
    , "2B_UMPIRE_ID"
    , "2B_UMPIRE_NAME"
    , "3B_UMPIRE_ID"
    , "3B_UMPIRE_NAME"
    , "LF_UMPIRE_ID"
    , "LF_UMPIRE_NAME"
    , "RF_UMPIRE_ID"
    , "RF_UMPIRE_NAME"
    , "VISITING_TEAM_MANAGER_ID"
    , "VISITING_TEAM_MANAGER_NAME"
    , "HOME_TEAM_MANAGER_ID"
    , "HOME_TEAM_MANAGER_NAME"
    , "WINNING_PITCHER_ID"
    , "WINNING_PITCHER_NAME"
    , "LOSING_PITCHER_ID"
    , "LOSING_PITCHER_NAME"
    , "SAVING_PITCHER_ID"
    , "SAVING_PITCHER_NAME"
    , "GAME_WINNING_RBI_BATTER_ID"
    , "GAME_WINNING_RBI_BATTER_NAME"
    , "VISITING_STARTING_PITCHER_ID"
    , "VISITING_STARTING_PITCHER_NAME"
    , "HOME_STARTING_PITCHER_ID"
    , "HOME_STARTING_PITCHER_NAME"
    , "VISITING_BATTER_1_ID"
    , "VISITING_BATTER_1_NAME"
    , "VISITING_BATTER_1_POS"
    , "VISITING_BATTER_2_ID"
    , "VISITING_BATTER_2_NAME"
    , "VISITING_BATTER_2_POS"
    , "VISITING_BATTER_3_ID"
    , "VISITING_BATTER_3_NAME"
    , "VISITING_BATTER_3_POS"
    , "VISITING_BATTER_4_ID"
    , "VISITING_BATTER_4_NAME"
    , "VISITING_BATTER_4_POS"
    , "VISITING_BATTER_5_ID"
    , "VISITING_BATTER_5_NAME"
    , "VISITING_BATTER_5_POS"
    , "VISITING_BATTER_6_ID"
    , "VISITING_BATTER_6_NAME"
    , "VISITING_BATTER_6_POS"
    , "VISITING_BATTER_7_ID"
    , "VISITING_BATTER_7_NAME"
    , "VISITING_BATTER_7_POS"
    , "VISITING_BATTER_8_ID"
    , "VISITING_BATTER_8_NAME"
    , "VISITING_BATTER_8_POS"
    , "VISITING_BATTER_9_ID"
    , "VISITING_BATTER_9_NAME"
    , "VISITING_BATTER_9_POS"
    , "HOME_BATTER_1_ID"
    , "HOME_BATTER_1_NAME"
    , "HOME_BATTER_1_POS"
    , "HOME_BATTER_2_ID"
    , "HOME_BATTER_2_NAME"
    , "HOME_BATTER_2_POS"
    , "HOME_BATTER_3_ID"
    , "HOME_BATTER_3_NAME"
    , "HOME_BATTER_3_POS"
    , "HOME_BATTER_4_ID"
    , "HOME_BATTER_4_NAME"
    , "HOME_BATTER_4_POS"
    , "HOME_BATTER_5_ID"
    , "HOME_BATTER_5_NAME"
    , "HOME_BATTER_5_POS"
    , "HOME_BATTER_6_ID"
    , "HOME_BATTER_6_NAME"
    , "HOME_BATTER_6_POS"
    , "HOME_BATTER_7_ID"
    , "HOME_BATTER_7_NAME"
    , "HOME_BATTER_7_POS"
    , "HOME_BATTER_8_ID"
    , "HOME_BATTER_8_NAME"
    , "HOME_BATTER_8_POS"
    , "HOME_BATTER_9_ID"
    , "HOME_BATTER_9_NAME"
    , "HOME_BATTER_9_POS"
    , "ADDL_INFO"
    , "ACQUISITION_INFO"
]

