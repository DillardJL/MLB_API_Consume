import requests
import pandas as pd
import sqlite3
import datetime


def playByPlayLoad(dbpath):

    # INITIALIZE VARIABLES
    numberofplays = 0
    playbyplaydf = pd.DataFrame(columns=['gameplaykey', 'gameid', 'playnumber', 'playresult', 'inning', 'istopinning',
                                         'batterid', 'batside', 'pitcherid', 'pitchhand','useincalc', 'reachedbase',
                                         'lastupdate'])
    playbyplayerrordf = pd.DataFrame(columns=['gameid', 'playnumber', 'ErrorMessage', 'ErrorLogDate'])
    # SET SQL INITIAL CONNECTION
    sqlite_file = dbpath
    conn = sqlite3.connect(sqlite_file)
    c = conn.cursor()
    sql = "select distinct gameid from games where isvalid = 1 and ispbpfinal = 0 and gamestate = \'Final\';"
    c.execute(sql)
    data = c.fetchall()

    # LOOP PROCESS FOR GAMES IN DB
    for row in data:
        gameid = str(row)
        gameid = str(gameid)
        gameid = str(gameid[1:int(len(str(row))-2)])

        # CALL API URL
        url = 'http://statsapi.mlb.com//api/v1//game/' + gameid + '/playByPlay'
        resp = requests.get(url)
        x = resp.json()

        # NUMBER OF PLAYS LOOP
        numberofplays = len(x['allPlays'])
        i = 0
        while i < numberofplays:
            now = str(datetime.datetime.now())
            now = now[0:19]


            playresult = x['allPlays'][i]['result']['event']
            inning = x['allPlays'][i]['about']['inning']
            istopinning = x['allPlays'][i]['about']['isTopInning']
            batterid = x['allPlays'][i]['matchup']['batter']['id']
            pitcherid = x['allPlays'][i]['matchup']['pitcher']['id']
            pitchhand = x['allPlays'][i]['matchup']['pitchHand']['code']
            batside = x['allPlays'][i]['matchup']['batSide']['code']

            playbyplaydf.loc[0] = [gameid+'_'+str(i), gameid, i, playresult, inning, istopinning, batterid, batside,
                                   pitcherid, pitchhand, 0, 0, now]

        # LOAD PlayByPlay DATAFRAME TO DB
            try:
                playbyplaydf.to_sql('PlayByPlay', conn, if_exists='append')
                conn.commit()
                pass

        # LOAD ERRORS INTO PlayByPlay ERROR TABLE
            except Exception as e:
                playbyplayerrordf.loc[0] = [gameid, str(i), str(e), now]
                playbyplayerrordf.to_sql('ErrorLog_playbyplaydf', conn, if_exists='append')
                pass
            i = i + 1

    conn.close()
    now = str(datetime.datetime.now())
    print('Play By Play Data Load Complete ', now[0:19])
