
BEGIN TRANSACTION;
DELETE FROM A_PitchingDFS;
COMMIT TRANSACTION;

BEGIN TRANSACTION;
INSERT INTO A_PitchingDFS
SELECT
PBS.playergamekey
,substr(PBS.playerid,3,(length(PBS.playerid)-2)) playerid
,PBS.gameid
,DATE(SUBSTR(G.gamedate, 1,10)) gamedate
,P.positionabbrev position
,PBS.earnedruns ER
,PBS.strikeouts K
,PBS.inningspitched I
,PBS.wins W
,((SUM((PBS.earnedruns)*-3) +
    (SUM((PBS.wins)*6)) +
        (SUM((PBS.strikeouts)*3)) +
            (SUM((PBS.inningspitched)*3)) +
                (CASE WHEN PBS.gamesstarted = 1 AND PBS.inningspitched >= 6 AND PBS.earnedruns <=3 THEN 4 else 0 END))) FanDuel_gameTotal
FROM
Pitching_BoxScore PBS
,Players P
,Games G
WHERE
P.playerid = SUBSTR(PBS.playerid,3,LENGTH(PBS.playerid)-2 )
AND G.gameid = PBS.gameid
AND P.positionabbrev = 'P'
GROUP BY PBS.playergamekey, PBS.playerid, PBS.gameid, G.gamedate, P.fullname, P.positionabbrev;
COMMIT TRANSACTION;

/*
CREATE TABLE A_PitchingDFS (
	playergamekey	TEXT,
	playerid	INTEGER,
	gameid	INTEGER,
	gamedate	TEXT,
	position	TEXT,
	FanDuel_gameTotal	NUMERIC
)
*/