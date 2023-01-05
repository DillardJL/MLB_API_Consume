import sqlite3 as db
import requests
import pandas as pd
import datetime


def pitchingBoxScore(dbpath):

    with db.connect(dbpath) as conn:
        cur = conn.cursor()
        sql = "SELECT distinct gameid FROM Games WHERE gamestate = 'Final' and isvalid = 1 and ispitchboxfinal = 0;"
        cur.execute(sql)
        data = cur.fetchall()

    for row in data:
        gameid = str(row)
        gameid = str(gameid)
        gameid = str(gameid[1:int(len(str(row))-2)])

        url = 'http://statsapi.mlb.com//api/v1/game/' + gameid + '/boxscore'
        resp = requests.get(url)
        data = resp.json()
        boxscoredf = pd.DataFrame(columns=['playergamekey', 'playerid', 'gameid', 'teamid', 'gamesPlayed',
                                           'gamesstarted', 'wins', 'earnedruns', 'strikeouts', 'inningspitched',
                                           'lastupdate'])
        pitchingerrordf = pd.DataFrame(columns=['playerID', 'GameID', 'ErrorMessage', 'ErrorLogDate'])

        # LOAD VARIABLES
        gamesPlayed = 0
        gamesStarted = 0
        wins = 0
        earnedRuns = 0
        strikeOuts = 0
        inningsPitched = 0

        tdata = data['teams']
        atdata = tdata['away']
        hdata = tdata['home']
        aplayers = atdata.copy()
        hplayers = hdata.copy()
        aplayers = aplayers['pitchers']
        hplayers = hplayers['pitchers']
        ateamid = atdata['team']['id']
        hteamid = hdata['team']['id']

        # AWAY LOOP
        length = len(aplayers)
        for i in range(length):
            now = str(datetime.datetime.now())
            now = now[0:19]
            x = 'ID' + str(aplayers[i])

            if x in (data['teams']['away']['players']):
                y = data['teams']['away']['players'][x]['stats']['pitching']
                try:
                    gamesplayed = y['gamesPlayed']
                except:
                    gamesplayed = 0
                try:
                    gamesStarted = y['gamesStarted']
                except:
                    gamesStarted = 0
                try:
                    wins = y['wins']
                except:
                    wins = 0
                try:
                    earnedRuns = y['earnedRuns']
                except:
                    earnedRuns = 0
                try:
                    strikeOuts = y['strikeOuts']
                except:
                    strikeOuts = 0
                try:
                    inningsPitched = y['inningsPitched']
                except:
                    inningsPitched = 0

            # LOAD DATA FRAME FOR DB UPLOAD
                boxscoredf.loc[0] = [x+'_'+gameid, x, gameid, ateamid, gamesPlayed, gamesStarted, wins, earnedRuns,
                                     strikeOuts, inningsPitched, now]
            # CATCH ERRORS ON SQL UPLOAD AND LOG
                try:
                    boxscoredf.to_sql('Pitching_BoxScore', conn, if_exists='append')
                    conn.commit()
                    pass
                except Exception as e:
                    now = str(datetime.datetime.now())
                    errorlist = (x, gameid, e, (now[0:19]))
                    pitchingerrordf.loc[0] = [x, gameid, str(e), now[0:19]]
                    pitchingerrordf.to_sql('ErrorLog_Pitching_BoxScore', conn, if_exists='append')
                    pass

        # HOME LOOP
        length = len(hplayers)
        for i in range(length):
            now = str(datetime.datetime.now())
            now = now[0:19]
            x = 'ID' + str(hplayers[i])

            if x in (data['teams']['home']['players']):
                y = data['teams']['home']['players'][x]['stats']['pitching']
                try:
                    gamesplayed = y['gamesPlayed']
                except:
                    gamesplayed = 0
                try:
                    gamesStarted = y['gamesStarted']
                except:
                    gamesStarted = 0
                try:
                    wins = y['wins']
                except:
                    wins = 0
                try:
                    earnedRuns = y['earnedRuns']
                except:
                    earnedRuns = 0
                try:
                    strikeOuts = y['strikeOuts']
                except:
                    strikeOuts = 0
                try:
                    inningsPitched = y['inningsPitched']
                except:
                    inningsPitched=0

            # LOAD DATA FRAME FOR DB UPLOAD
                boxscoredf.loc[0] = [x+'_'+gameid, x, gameid, hteamid, gamesPlayed, gamesStarted, wins, earnedRuns,
                                     strikeOuts, inningsPitched, now]
            # CATCH ERRORS ON SQL UPLOAD AND LOG
                try:
                    boxscoredf.to_sql('Pitching_BoxScore', conn, if_exists='append')
                    conn.commit()
                    pass
                except Exception as e:
                    now = str(datetime.datetime.now())
                    errorlist = (x, gameid, e, (now[0:19]))
                    pitchingerrordf.loc[0] = [x, gameid, str(e), now[0:19]]
                    pitchingerrordf.to_sql('ErrorLog_Pitching_BoxScore', conn, if_exists='append')
                    pass

        # SET Pitching BOX SCORE FLAG (ispitchboxfinal) = 1
        cur1 = conn.cursor()
        lsc_sql = "UPDATE GAMES SET ispitchboxfinal = 1 WHERE gameid = " + gameid + ";"
        cur1.execute(lsc_sql)
        conn.commit()

    conn.close()
    now = str(datetime.datetime.now())
    print('Pitching Boxscore Upload Complete ', now[0:19])
