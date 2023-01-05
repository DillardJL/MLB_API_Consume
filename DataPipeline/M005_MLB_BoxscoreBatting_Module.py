import sqlite3 as db
import requests
import pandas as pd
import datetime


def battingBoxScore(dbpath):


    with db.connect(dbpath) as conn:
        cur = conn.cursor()
        sql = "SELECT distinct gameid FROM Games WHERE gamestate = 'Final' and isvalid = 1 and isbatboxfinal = 0;"
        cur.execute(sql)
        data = cur.fetchall()

    for row in data:
        gameid = str(row)
        gameid = str(gameid)
        gameid = str(gameid[1:int(len(str(row))-2)])
        url = 'http://statsapi.mlb.com//api/v1/game/' + gameid + '/boxscore'
        resp = requests.get(url)
        data = resp.json()
        boxscoredf = pd.DataFrame(columns=['playergamekey', 'playerid', 'gameid', 'gamesPlayed', 'flyOuts',
                                           'groundOuts', 'runs', 'doubles', 'triples', 'homeRuns', 'strikeOuts',
                                           'baseOnBalls', 'intentionalWalks', 'hits', 'hitByPitch', 'atBats',
                                           'caughtStealing', 'stolenBases', 'groundIntoDoublePlay',
                                           'groundIntoTriplePlay', 'totalBases', 'rbi', 'leftOnBase', 'sacBunts',
                                           'sacFlies', 'catchersInterference', 'pickoffs', 'lastupdate'])
        battingerrordf = pd.DataFrame(columns=['playerID', 'GameID', 'ErrorMessage', 'ErrorLogDate'])

        tdata = data['teams']
        atdata = tdata['away']
        hdata = tdata['home']
        aplayers = atdata.copy()
        hplayers = hdata.copy()
        aplayers = aplayers['batters']
        hplayers = hplayers['batters']

        # AWAY LOOP
        home_away = 'away'
        awaylength = len(aplayers)
        for i in range(awaylength):
            playerid = 'ID' + str(aplayers[i])
            # Build temporary data frame in order to check if there is batter data - if not, skip playerid
            dataframe = data['teams'][home_away]['players'][playerid]['stats']['batting']
            dataframe = pd.DataFrame(list(dataframe.items()))
            dataframe = dataframe.T
            if not dataframe.empty:
                gamesPlayed = data['teams'][home_away]['players'][playerid]['stats']['batting']['gamesPlayed']
                flyOuts = data['teams'][home_away]['players'][playerid]['stats']['batting']['flyOuts']
                groundOuts = data['teams'][home_away]['players'][playerid]['stats']['batting']['groundOuts']
                runs = data['teams'][home_away]['players'][playerid]['stats']['batting']['runs']
                doubles = data['teams'][home_away]['players'][playerid]['stats']['batting']['doubles']
                triples = data['teams'][home_away]['players'][playerid]['stats']['batting']['triples']
                homeRuns = data['teams'][home_away]['players'][playerid]['stats']['batting']['homeRuns']
                strikeOuts = data['teams'][home_away]['players'][playerid]['stats']['batting']['strikeOuts']
                baseOnBalls = data['teams'][home_away]['players'][playerid]['stats']['batting']['baseOnBalls']
                intentionalWalks = data['teams'][home_away]['players'][playerid]['stats']['batting']['intentionalWalks']
                hits = data['teams'][home_away]['players'][playerid]['stats']['batting']['hits']
                hitByPitch = data['teams'][home_away]['players'][playerid]['stats']['batting']['hitByPitch']
                atBats = data['teams'][home_away]['players'][playerid]['stats']['batting']['atBats']
                caughtStealing = data['teams'][home_away]['players'][playerid]['stats']['batting']['caughtStealing']
                stolenBases = data['teams'][home_away]['players'][playerid]['stats']['batting']['stolenBases']
                groundIntoDoublePlay = data['teams'][home_away]['players'][playerid]['stats']['batting']['groundIntoDoublePlay']
                groundIntoTriplePlay = data['teams'][home_away]['players'][playerid]['stats']['batting']['groundIntoTriplePlay']
                totalBases = data['teams'][home_away]['players'][playerid]['stats']['batting']['totalBases']
                rbi = data['teams'][home_away]['players'][playerid]['stats']['batting']['rbi']
                leftOnBase = data['teams'][home_away]['players'][playerid]['stats']['batting']['leftOnBase']
                sacBunts = data['teams'][home_away]['players'][playerid]['stats']['batting']['sacBunts']
                sacFlies = data['teams'][home_away]['players'][playerid]['stats']['batting']['sacFlies']
                catchersInterference = data['teams'][home_away]['players'][playerid]['stats']['batting']['catchersInterference']
                pickoffs = data['teams'][home_away]['players'][playerid]['stats']['batting']['pickoffs']

                now = str(datetime.datetime.now())
                boxscoredf.loc[0] = [playerid + '_' + gameid, playerid, gameid, gamesPlayed, flyOuts, groundOuts, runs,
                                     doubles, triples, homeRuns, strikeOuts, baseOnBalls, intentionalWalks, hits,
                                     hitByPitch, atBats, caughtStealing, stolenBases, groundIntoDoublePlay,
                                     groundIntoTriplePlay, totalBases, rbi, leftOnBase, sacBunts, sacFlies,
                                     catchersInterference, pickoffs, now[0:19]]

                try:
                    boxscoredf.to_sql('Batting_BoxScore', conn, if_exists='append')
                    conn.commit()
                    pass

                except Exception as e:
                    # create a table ErrorLog_Batting_BoxScore and load with PlayerID & GameID & exception(e)
                    now = str(datetime.datetime.now())
                    errorlist = (playerid, gameid, str(e), (now[0:19]))
                    battingerrordf.loc[0] = [playerid, gameid, str(e), now[0:19]]
                    battingerrordf.to_sql('ErrorLog_Batting_BoxScore', conn, if_exists='append')
                    pass

        # HOME LOOP
        home_away = 'home'
        homelength = len(hplayers)
        for i in range(homelength):
            playerid = 'ID' + str(hplayers[i])
            # Build temporary data frame in order to check if there is batter data - if not, skip playerid
            dataframe = data['teams'][home_away]['players'][playerid]['stats']['batting']
            dataframe = pd.DataFrame(list(dataframe.items()))
            dataframe = dataframe.T
            if not dataframe.empty:
                gamesPlayed = data['teams'][home_away]['players'][playerid]['stats']['batting']['gamesPlayed']
                flyOuts = data['teams'][home_away]['players'][playerid]['stats']['batting']['flyOuts']
                groundOuts = data['teams'][home_away]['players'][playerid]['stats']['batting']['groundOuts']
                runs = data['teams'][home_away]['players'][playerid]['stats']['batting']['runs']
                doubles = data['teams'][home_away]['players'][playerid]['stats']['batting']['doubles']
                triples = data['teams'][home_away]['players'][playerid]['stats']['batting']['triples']
                homeRuns = data['teams'][home_away]['players'][playerid]['stats']['batting']['homeRuns']
                strikeOuts = data['teams'][home_away]['players'][playerid]['stats']['batting']['strikeOuts']
                baseOnBalls = data['teams'][home_away]['players'][playerid]['stats']['batting']['baseOnBalls']
                intentionalWalks = data['teams'][home_away]['players'][playerid]['stats']['batting']['intentionalWalks']
                hits = data['teams'][home_away]['players'][playerid]['stats']['batting']['hits']
                hitByPitch = data['teams'][home_away]['players'][playerid]['stats']['batting']['hitByPitch']
                atBats = data['teams'][home_away]['players'][playerid]['stats']['batting']['atBats']
                caughtStealing = data['teams'][home_away]['players'][playerid]['stats']['batting']['caughtStealing']
                stolenBases = data['teams'][home_away]['players'][playerid]['stats']['batting']['stolenBases']
                groundIntoDoublePlay = data['teams'][home_away]['players'][playerid]['stats']['batting']['groundIntoDoublePlay']
                groundIntoTriplePlay = data['teams'][home_away]['players'][playerid]['stats']['batting']['groundIntoTriplePlay']
                totalBases = data['teams'][home_away]['players'][playerid]['stats']['batting']['totalBases']
                rbi = data['teams'][home_away]['players'][playerid]['stats']['batting']['rbi']
                leftOnBase = data['teams'][home_away]['players'][playerid]['stats']['batting']['leftOnBase']
                sacBunts = data['teams'][home_away]['players'][playerid]['stats']['batting']['sacBunts']
                sacFlies = data['teams'][home_away]['players'][playerid]['stats']['batting']['sacFlies']
                catchersInterference = data['teams'][home_away]['players'][playerid]['stats']['batting']['catchersInterference']
                pickoffs = data['teams'][home_away]['players'][playerid]['stats']['batting']['pickoffs']

                now = str(datetime.datetime.now())
                boxscoredf.loc[0] = [playerid + '_' + gameid, playerid, gameid, gamesPlayed, flyOuts, groundOuts, runs,
                                     doubles, triples, homeRuns, strikeOuts, baseOnBalls, intentionalWalks, hits,
                                     hitByPitch, atBats, caughtStealing, stolenBases, groundIntoDoublePlay,
                                     groundIntoTriplePlay, totalBases, rbi, leftOnBase, sacBunts, sacFlies,
                                     catchersInterference, pickoffs, now[0:19]]

                try:
                    boxscoredf.to_sql('Batting_BoxScore', conn, if_exists='append')
                    conn.commit()
                    pass

                except Exception as e:
                    # create a table ErrorLog_Batting_BoxScore and load with PlayerID & GameID & exception(e)
                    now = str(datetime.datetime.now())
                    errorlist = (playerid, gameid, str(e), (now[0:19]))
                    battingerrordf.loc[0] = [playerid, gameid, str(e), now[0:19]]
                    battingerrordf.to_sql('ErrorLog_Batting_BoxScore', conn, if_exists='append')
                    pass

        # SET BATTING BOX SCORE FLAG (isbatboxfinal) = 1
        cur1 = conn.cursor()
        lsc_sql = "UPDATE GAMES SET isbatboxfinal = 1 WHERE gameid = " + gameid + ";"
        cur1.execute(lsc_sql)
        conn.commit()

    conn.close()
    now = str(datetime.datetime.now())
    print('Batting Box Score Data Load Complete ', now[0:19])
