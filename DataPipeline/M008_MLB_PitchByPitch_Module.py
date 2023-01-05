import sqlite3 as db
import pandas as pd
import requests
import datetime


def pitchbypitch(dbpath):


    # INITIALIZE VARIABLES
    numberofplays = 0
    numberofpitches = 0
    pitchbypitch = pd.DataFrame(columns=['gameplaypitchkey', 'gameplay', 'playpitch', 'ispitch', 'isstrike', 'isball',
                                         'callcode', 'calldescription', 'pitchtypecode', 'pitchtypedescription',
                                         'pitchstartspeed', 'pitchendspeed', 'strikezonetop', 'strikezonebottom',
                                         'hittrajectory', 'hithardness', 'hittotaldistance', 'launchspeed',
                                         'launchangle', 'isinplay', 'zone', 'breakangle', 'breaklength', 'breakY',
                                         'spinrate', 'spindirection', 'extension', 'lastupdate'])
    pitchbypitcherrordf = pd.DataFrame(columns=['gameplaypitchkey', 'GameID', 'ErrorMessage', 'ErrorLogDate'])


    # SET SQL INITIAL CONNECTION
    sqlite_file = dbpath
    conn = db.connect(sqlite_file)
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

        # LOOP FOR NUMBER OF PLAYS
        numberofplays = len(x['allPlays'])
        playnum = 0
        while playnum < numberofplays:
            now = str(datetime.datetime.now())
            now = now[0:19]
            numberofpitches = len(x['allPlays'][playnum]['playEvents'])
            pitchnum = 0

        # LOOP FOR NUMBER OF PITCHES IN PLAYS
            while pitchnum < numberofpitches:
            # SET DATAFRAME VARIABLES
                # ##isptich
                try:
                    ispitch = x['allPlays'][playnum]['playEvents'][pitchnum]['isPitch']
                except:
                    ispitch = 0

                # ##isinplay
                try:
                    isinplay = x['allPlays'][playnum]['playEvents'][pitchnum]['details']['isInPlay']
                except:
                    isinplay = 0

                # ##isstrike
                try:
                    isstrike = x['allPlays'][playnum]['playEvents'][pitchnum]['details']['isStrike']
                except:
                    isstrike = 0

                # ##isball
                try:
                    isball = x['allPlays'][playnum]['playEvents'][pitchnum]['details']['isBall']
                except:
                    isball = 0

                # ##callcode
                try:
                    callcode = x['allPlays'][playnum]['playEvents'][pitchnum]['details']['call']['code']
                except:
                    callcode = None

                # ##calldescription
                try:
                    calldescription = x['allPlays'][playnum]['playEvents'][pitchnum]['details']['call']['description']
                except:
                    calldescription = None

                # ##pitchtypecode
                try:
                    pitchtypecode = x['allPlays'][playnum]['playEvents'][pitchnum]['details']['type']['code']
                except:
                    pitchtypecode = None

                # ##pitchtypedescription
                try:
                    pitchtypedescription = x['allPlays'][playnum]['playEvents'][pitchnum]['details']['type']['description']
                except:
                    pitchtypedescription = None

                # ##pitchstartspeed
                try:
                    pitchstartspeed = x['allPlays'][playnum]['playEvents'][pitchnum]['pitchData']['startSpeed']
                except:
                    pitchstartspeed = 0

                # ##pitchendspeed
                try:
                    pitchendspeed = x['allPlays'][playnum]['playEvents'][pitchnum]['pitchData']['endSpeed']
                except:
                    pitchendspeed = 0

                # ##strikezonetop
                try:
                    strikezonetop = x['allPlays'][playnum]['playEvents'][pitchnum]['pitchData']['strikeZoneTop']
                except:
                    strikezonetop = 0

                # ##strikezonebottom
                try:
                    strikezonebottom = x['allPlays'][playnum]['playEvents'][pitchnum]['pitchData']['strikeZoneBottom']
                except:
                    strikezonebottom = 0

                # ##hittrajectory
                try:
                    hittrajectory = x['allPlays'][playnum]['playEvents'][pitchnum]['hitData']['trajectory']
                except:
                    hittrajectory = None

                # ##hithardness
                try:
                    hithardness = x['allPlays'][playnum]['playEvents'][pitchnum]['hitData']['hardness']
                except:
                    hithardness = None

                # ##hittotaldistance
                try:
                    hittotaldistance = x['allPlays'][playnum]['playEvents'][pitchnum]['hitData']['totalDistance']
                except:
                    hittotaldistance = 0

                # ##launchspeed
                try:
                    launchspeed = x['allPlays'][playnum]['playEvents'][pitchnum]['hitData']['launchSpeed']
                except:
                    launchspeed = None

                # ##launchangle
                try:
                    launchangle = x['allPlays'][playnum]['playEvents'][pitchnum]['hitData']['launchAngle']
                except:
                    launchangle = None

                # ##isinplay
                try:
                    isinplay = x['allPlays'][playnum]['playEvents'][pitchnum]['details']['isInPlay']
                except:
                    isinplay = None

                # ##zone
                try:
                    zone = x['allPlays'][playnum]['playEvents'][pitchnum]['pitchData']['zone']
                except:
                    zone = None

                # ##breakangle
                try:
                    breakangle = x['allPlays'][playnum]['playEvents'][pitchnum]['pitchData']['breaks']['breakAngle']
                except:
                    breakangle = None

                # ##breaklength
                try:
                    breaklength = x['allPlays'][playnum]['playEvents'][pitchnum]['pitchData']['breaks'][
                        'breakLength']
                except:
                    breaklength = None

                # ##breakY
                try:
                    breakY = x['allPlays'][playnum]['playEvents'][pitchnum]['pitchData']['breaks']['breakY']
                except:
                    breakY = None

                # ##spinrate
                try:
                    spinrate = x['allPlays'][playnum]['playEvents'][pitchnum]['pitchData']['breaks']['spinRate']
                except:
                    spinrate = None

                # ##spinDirection
                try:
                    spindirection = x['allPlays'][playnum]['playEvents'][pitchnum]['pitchData']['breaks'][
                        'spinDirection']
                except:
                    spindirection = None

                # ##extension
                try:
                    extension = x['allPlays'][playnum]['playEvents'][pitchnum]['pitchData']['extension']
                except:
                    extension = None

                # LOAD DATAFRAME
                pitchbypitch.loc[0] = [str(gameid) + '_' + str(playnum) + '_' + str(pitchnum),
                                       str(gameid) + '_' + str(playnum),
                                       str(playnum) + '_' + str(pitchnum), ispitch, isstrike, isball, callcode,
                                       calldescription, pitchtypecode, pitchtypedescription, pitchstartspeed,
                                       pitchendspeed, strikezonetop, strikezonebottom, hittrajectory, hithardness,
                                       hittotaldistance, launchspeed, launchangle, isinplay, zone, breakangle,
                                       breaklength, breakY, spinrate, spindirection, extension, now]

            # LOAD PitchByPitch DATAFRAME TO DB
                try:
                    pitchbypitch.to_sql('PitchByPitch', conn, if_exists='append')
                    conn.commit()
                    pass
                except Exception as e:
            # LOAD ERRORS INTO PitchByPitch ERROR TABLE
                    pitchbypitcherrordf.loc[0] = [str(gameid)+'_'+str(playnum)+'_'+str(pitchnum), gameid, str(e), now]
                    pitchbypitcherrordf.to_sql('ErrorLog_pitchbypitch', conn, if_exists='append')
                    pass
                pitchnum = pitchnum + 1
            playnum = playnum + 1

            # SET PlayByPlay & PitchByPitch Flag (ispbpfinal) = 1
            cur1 = conn.cursor()
            lsc_sql = "UPDATE GAMES SET ispbpfinal = 1 WHERE gameid = " + gameid + ";"
            cur1.execute(lsc_sql)
            conn.commit()

    conn.close()
    now = str(datetime.datetime.now())
    print('Pitch By Pitch Data Load Complete ', now[0:19])
