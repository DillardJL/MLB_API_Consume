
BEGIN TRANSACTION;
DELETE FROM A_BattingDFS;
COMMIT TRANSACTION;


BEGIN TRANSACTION;
INSERT INTO A_BattingDFS
SELECT
BBS.playergamekey
,substr(BBS.playerid,3,(length(BBS.playerid)-2)) playerid
,BBS.gameid
,date(substr(G.gamedate, 1,10)) gamedate
,P.positionabbrev position
,sum(BBS.atBats) atbats
,sum(BBS.hits) b1
,sum(BBS.doubles) b2
,sum(BBS.triples) b3
,sum(BBS.homeruns) hr
,sum(BBS.rbi) rbi
,sum(BBS.runs) runs
,sum(BBS.baseonballs) bb
,sum(BBS.stolenbases) sb
,(((sum(BBS.hits) - (sum(BBS.doubles)+sum(BBS.triples)+sum(BBS.homeruns)+sum(BBS.baseonballs)))*3) +
	(sum(BBS.doubles)*6) +
	(sum(BBS.triples)*9) +
	(sum(BBS.homeruns)*12) +
	(sum(BBS.rbi)*3.5) +
	(sum(BBS.runs)*3.2 ) +
	(sum(BBS.baseonballs)*3) +
	(sum(BBS.stolenbases)*6) +
	(sum(BBS.hitbypitch)*3)) FanDuel_gameTotal
FROM Batting_BoxScore BBS,
Players P
,Games G
WHERE P.playerid = substr(BBS.playerid,3,length(BBS.playerid)-2 )
AND G.gameid = BBS.gameid
GROUP BY BBS.playergamekey, BBS.playergamekey, BBS.playerid, BBS.gameid, G.gamedate, P.fullname, P.positionabbrev;
COMMIT TRANSACTION;

/*
CREATE TABLE A_BattingDFS (
	playergamekey TEXT,
	playerid	INTEGER,
	gameid	INTEGER,
	gamedate	TEXT,
	position	TEXT,
	hits	NUMERIC,
	atbats	NUMERIC,
	homeruns	NUMERIC,
	FanDuel_gameTotal	NUMERIC
)
*/