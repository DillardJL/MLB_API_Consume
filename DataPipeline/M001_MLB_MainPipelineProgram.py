from datetime import date, timedelta

# SET PATHS
dbpath = ''
path = ''
sqlfilepath = ''

# SET WORKING DIRECTORY AND IMPORT PROCEDURES
import M002_MLB_GameSchedule_Module
import M003_MLB_GameAttributeUpdate_Module
import M004_MLB_BattingOrderLineup_Module
import M005_MLB_BoxscoreBatting_Module
import M006_MLB_BoxscorePitching_Module
import M007_MLB_PlayByPlay_Module
import M008_MLB_PitchByPitch_Module
import M009_MLB_Players_Module
import M010_MLB_FlowFunctions as Flow

# TAKE PROMPTS FROM USER
d1 = date.today() - timedelta(days=10)
d2 = date.today() + timedelta(days=1)
GameScheduleStartDT = d1.strftime("%Y-%m-%d")
GameScheduleEndDT = d2.strftime("%Y-%m-%d")
#######################################################
# ## STEP 1 - RUN GAMES SCHEDULE TO LOAD UPCOMING GAMES
M002_MLB_GameSchedule_Module.GameSchedule(dbpath, GameScheduleStartDT, GameScheduleEndDT)
# STEP 1a - SET VALID WHERE BOTH TEAMS 103 OR 104 AND NOT SPRING TRAINING, ALL-STAR GAME, OR EXHIBITION
Flow.setValidGames(dbpath)
# STEP 1b - ARE PROBABLE PITCHERS VALID PLAYERS
M009_MLB_Players_Module.LoadNewPlayers(dbpath)
# STEP 1c - REMOVE UNIQUE CONSTRAINT ERROR MESSAGES FROM LOG
Flow.clearGamesErrors(dbpath)
#####################################################################################
# ## STEP 2 - UPDATE GAME HEADER DATA AND WEATHER FORECAST FOR GAMES NOT YET COMPLETED
M003_MLB_GameAttributeUpdate_Module.GameAttributeUpdate(dbpath)
# ## STEP 3 - LOAD/UPDATE BATTING ORDER LINEUPS
M004_MLB_BattingOrderLineup_Module.battingOrderLineup(dbpath)
# STEP 3a - REMOVE UNIQUE CONSTRAINT ERROR MESSAGES FROM LOG
Flow.clearBattingBoxErrors(dbpath)
###########################################################
# ## STEP 5 - LOAD BATTING BOX SCORE FOR COMPLETED GAMES
M005_MLB_BoxscoreBatting_Module.battingBoxScore(dbpath)
# STEP 5a - REMOVE UNIQUE CONSTRAINT ERROR MESSAGES FROM LOG
Flow.clearBattingBoxErrors(dbpath)
#########################################################
# ## STEP 6 - LOAD PITCHING BOX SCORE FOR COMPLETED GAMES
M006_MLB_BoxscorePitching_Module.pitchingBoxScore(dbpath)
# STEP 6a - REMOVE UNIQUE CONSTRAINT ERROR MESSAGES FROM LOG
Flow.clearPitchingBoxErrors(dbpath)
########################################################
# ## STEP 7 - LOAD PITCH-BY=PITCH & PLAY-BY-PLAY DATA FOR COMPLETED GAMES
M007_MLB_PlayByPlay_Module.playByPlayLoad(dbpath)
M008_MLB_PitchByPitch_Module.pitchbypitch(dbpath)
Flow.setUseInPlayCalc(dbpath)
####################################################
# ## STEP 9 - LOAD PLAYER DATA FOR NEW PLAYERS FOUND
M009_MLB_Players_Module.LoadNewPlayers(dbpath)
#####################################################
# ## STEP 10 - REBUILD BATTER & PITCHER DFS TABLES
# These tables show the calculations of DFS scores based on attributes
Flow.rebuildBattingDFS(dbpath, sqlfilepath)
Flow.rebuildPitchingDFS(dbpath, sqlfilepath)
