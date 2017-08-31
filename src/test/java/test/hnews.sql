create table hnews (
	-- The username of the item's author.
	by		varchar(256)	null,	
	-- Story score
	score	INTEGER		null,	
	-- Unix time
	time	INTEGER	null,	
	-- Timestamp for the unix time
	timestamp	TIMESTAMP	null,	
	-- Story title
	title	varchar(256)	null,
	-- Type of details (comment, comment_ranking, poll, story, job, pollopt)
	type	varchar(256)	null,
	-- Story url
	url	varchar(256)	null,
	-- Story or comment text
	text	varchar(100000)	null,
	-- Parent comment ID
	parent	INTEGER	null,
	-- Is deleted?
	deleted	BOOLEAN	null,
	-- Is dead?
	dead	BOOLEAN	null,
	-- Number of story or poll descendants
	descendants	INTEGER	null,	
	-- The item's unique id.
	id	INTEGER	null,
	-- Comment ranking
	ranking	INTEGER	null
)
