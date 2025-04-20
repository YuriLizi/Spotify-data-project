-- 1. What is the most liked episode?
SELECT
    episode_id,
    COUNT(*) AS like_count
FROM
    user_actions
WHERE
    action = 'like'
GROUP BY
    episode_id
ORDER BY
    like_count DESC
LIMIT 10;

-- 2. Which episode was most listened to?
SELECT
    episode_id,
    COUNT(*) AS listen_count
FROM
    user_actions
WHERE
    action = 'listen'
GROUP BY
    episode_id
ORDER BY
    listen_count DESC
LIMIT 10;

-- 3. Which episodes were most searched for?
SELECT
    episode_id,
    COUNT(*) AS search_count
FROM
    user_actions
WHERE
    action = 'search'
GROUP BY
    episode_id
ORDER BY
    search_count DESC
LIMIT 10;


-- Combined statistics for all actions
SELECT
    episode_id,
    SUM(CASE WHEN action = 'like' THEN 1 ELSE 0 END) AS likes,
    SUM(CASE WHEN action = 'listen' THEN 1 ELSE 0 END) AS listens,
    SUM(CASE WHEN action = 'search' THEN 1 ELSE 0 END) AS searches,
    COUNT(*) AS total_actions
FROM
    user_actions
GROUP BY
    episode_id
ORDER BY
    total_actions DESC
LIMIT 120;