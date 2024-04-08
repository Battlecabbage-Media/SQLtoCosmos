/*
T-SQL for Stored Procedures to be used with the SQL media to Cosmos DB migration.

*/

/*
-- Create schema for all of the stored procedures.
CREATE SCHEMA [cosmos]
GO

-- Give read and execute permissions to the readonlyappuser
GRANT SELECT ON SCHEMA :: cosmos TO readonlyappuser WITH GRANT OPTION;  
GRANT EXECUTE ON SCHEMA :: cosmos TO readonlyappuser WITH GRANT OPTION;  

/*

/*
--Cleanup Existing Stored Procedures
DROP PROCEDURE IF EXISTS [cosmos].[GetMoviesJson];
DROP PROCEDURE IF EXISTS [cosmos].[GetActorsEmbeddedJson];
DROP PROCEDURE IF EXISTS [cosmos].[GetDirectorsEmbeddedJson];
DROP PROCEDURE IF EXISTS [cosmos].[GetActorsReferenceJson];
DROP PROCEDURE IF EXISTS [cosmos].[GetDirectorsReferenceJson];
DROP PROCEDURE IF EXISTS [cosmos].[GetActorsHybridJson];
DROP PROCEDURE IF EXISTS [cosmos].[GetDirectorsHybridJson];
DROP PROCEDURE IF EXISTS [cosmos].[GetNewMoviesActors];
DROP PROCEDURE IF EXISTS [cosmos].[GetNewMoviesDirectors];
*/

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [cosmos].[GetMoviesJson]
    @date datetime
AS
BEGIN
    -- All movies and their details in single json results
    SELECT [value]
    FROM OPENJSON((
        SELECT
            m.external_id as id,
            m.title,
            'movie' as type,
            m.tagline,
            m.description,
            m.mpaa_rating,
            m.release_date,
            m.poster_url,
            (
                SELECT 
                    genres.genre as name
                    , CONCAT('gen',genres.genre_id) as id 
                FROM genres genres 
                WHERE m.genre_id = genres.genre_id
                FOR JSON PATH
            ) as genres,
            (
                SELECT 
                    actors.actor as name
                    , CONCAT('act',actors.actor_id) as id 
                FROM actorstomoviesjoin ma 
                INNER JOIN actors actors ON ma.actor_id = actors.actor_id
                WHERE ma.movie_id = m.movie_id
                FOR JSON PATH
            ) as actors,
            (
                SELECT 
                    directors.director as name
                    , CONCAT('dir',directors.director_id) as id 
                FROM directorstomoviesjoin md 
                INNER JOIN directors directors ON md.director_id = directors.director_id
                WHERE md.movie_id = m.movie_id
                FOR JSON PATH
            ) as directors
        FROM
            movies as m
        JOIN genres genres on m.genre_id = genres.genre_id
        WHERE m.release_date > @date
        FOR JSON PATH
    ))
END
GO


CREATE PROCEDURE [cosmos].[GetActorsEmbeddedJson]
    @date datetime
AS
BEGIN
-- Actors Embedded, all movie details in separate json results
    SELECT [value]
    FROM OPENJSON((
        SELECT
            CONCAT('mov',m.external_id,'act',actors.actor_id) as id,
            actors.actor as title,
            'actor' as type,
            m.external_id as movie_id,
            m.title as movie_title,
            m.tagline,
            m.description,
            m.mpaa_rating,
            m.release_date,
            m.poster_url,
            (
                SELECT 
                    genres.genre as name
                    , CONCAT('gen',genres.genre_id) as id 
                FROM genres genres 
                WHERE m.genre_id = genres.genre_id
                FOR JSON PATH
            ) as genres,
            (
                SELECT 
                    actors.actor as name
                    , CONCAT('act',actors.actor_id) as id 
                FROM actorstomoviesjoin ma 
                INNER JOIN actors actors ON ma.actor_id = actors.actor_id
                WHERE ma.movie_id = m.movie_id
                FOR JSON PATH
            ) as actors,
            (
                SELECT 
                    directors.director as name
                    , CONCAT('dir',directors.director_id) as id 
                FROM directorstomoviesjoin md 
                INNER JOIN directors directors ON md.director_id = directors.director_id
                WHERE md.movie_id = m.movie_id
                FOR JSON PATH
            ) as directors
        FROM
            movies as m
        JOIN genres genres on m.genre_id = genres.genre_id
        INNER JOIN  actorstomoviesjoin ma ON m.movie_id = ma.movie_id
        INNER JOIN actors actors ON ma.actor_id = actors.actor_id
        WHERE m.release_date > @date
        FOR JSON PATH
    ))
END
GO


CREATE PROCEDURE [cosmos].[GetDirectorsEmbeddedJson]
    @date datetime
AS
BEGIN
    -- Directors embedded, all movie details in separate json results
    SELECT [value]
    FROM OPENJSON((
        SELECT
            CONCAT('mov',m.external_id,'dir',directors.director_id) as id,
            directors.director as title,
            'director' as type,
            m.external_id as movie_id,
            m.title as movie_title,
            m.tagline,
            m.description,
            m.mpaa_rating,
            m.release_date,
            m.poster_url,
            (
                SELECT 
                    genres.genre as name
                    , CONCAT('gen',genres.genre_id) as id 
                FROM genres genres 
                WHERE m.genre_id = genres.genre_id
                FOR JSON PATH
            ) as genres,
            (
                SELECT 
                    actors.actor as name
                    , CONCAT('act',actors.actor_id) as id 
                FROM actorstomoviesjoin ma 
                INNER JOIN actors actors ON ma.actor_id = actors.actor_id
                WHERE ma.movie_id = m.movie_id
                FOR JSON PATH
            ) as actors,
            (
                SELECT 
                    directors.director as name
                    , CONCAT('dir',directors.director_id) as id 
                FROM directorstomoviesjoin md 
                INNER JOIN directors directors ON md.director_id = directors.director_id
                WHERE md.movie_id = m.movie_id
                FOR JSON PATH
            ) as directors
        FROM
            movies as m
        JOIN genres genres on m.genre_id = genres.genre_id
        INNER JOIN  directorstomoviesjoin md ON m.movie_id = md.movie_id
        INNER JOIN directors directors ON md.director_id = directors.director_id
        WHERE m.release_date > @date
        FOR JSON PATH
    ))
END
GO

CREATE PROCEDURE [cosmos].[GetActorsReferenceJson]
    @date datetime
AS
BEGIN
    -- Actors Reference, small result for each movie
    SELECT [value]
    FROM OPENJSON((
        SELECT
            CONCAT('mov',m.external_id,'act',actors.actor_id) as id,
            actors.actor as title,
            'actor' as type,
            m.external_id as movie_id,
            m.title as movie_title,
            m.mpaa_rating,
            m.release_date,
            m.poster_url
        FROM
            movies as m
        INNER JOIN  actorstomoviesjoin ma ON m.movie_id = ma.movie_id
        INNER JOIN actors actors ON ma.actor_id = actors.actor_id
        WHERE m.release_date > @date
        FOR JSON PATH
    ))
END
GO


CREATE PROCEDURE [cosmos].[GetDirectorsReferenceJson]
    @date datetime
AS
BEGIN
    -- Directors Reference, small result for each movie
    SELECT [value]
    FROM OPENJSON((
        SELECT
            CONCAT('mov',m.external_id,'dir',directors.director_id) as id,
            directors.director as title,
            'director' as type,
            m.external_id as movie_id,
            m.title as movie_title,
            m.mpaa_rating,
            m.release_date,
            m.poster_url
        FROM
            movies as m
        INNER JOIN  directorstomoviesjoin md ON m.movie_id = md.movie_id
        INNER JOIN directors directors ON md.director_id = directors.director_id
        WHERE m.release_date > @date
        FOR JSON PATH
    ))
END
GO


CREATE PROCEDURE [cosmos].[GetActorsHybridJson]
    @actor_id int
AS
BEGIN
    -- Actor Hybrid, all movies are in a single json array
    SELECT [value]
    FROM OPENJSON((
        SELECT
            CONCAT('act',a.actor_id) as id,
            a.actor as title,
            'person' as type,
            (
                SELECT 
                    m.external_id as movie_id,
                    m.title as movie_title,
                    m.mpaa_rating,
                    m.release_date,
                    'actor' as role
                FROM
                    movies as m
                INNER JOIN actorstomoviesjoin ma ON m.movie_id = ma.movie_id
                WHERE ma.actor_id = a.actor_id
                FOR JSON PATH
            ) as roles
        FROM
            actors as a
        WHERE a.actor_id = @actor_id
        FOR JSON PATH
    ))
END
GO

CREATE PROCEDURE [cosmos].[GetDirectorsHybridJson]
    @director_id int
AS
BEGIN
    -- Directors hybrid, all movies are in a single json array
    SELECT [value]
    FROM OPENJSON((
        SELECT
            CONCAT('dir',d.director_id) as id,
            d.director as title,
            'person' as type,
            (
                SELECT 
                    m.external_id as movie_id,
                    m.title as movie_title,
                    m.mpaa_rating,
                    m.release_date,
                    'director' as role
                FROM
                    movies as m
                INNER JOIN directorstomoviesjoin md ON m.movie_id = md.movie_id
                WHERE md.director_id = d.director_id
                FOR JSON PATH
            ) as roles
        FROM
            directors as d
        WHERE d.director_id = @director_id
        FOR JSON PATH
    ))
END
GO


CREATE PROCEDURE [cosmos].[GetNewMoviesActors]
    @date datetime
AS
BEGIN
    -- Actors from new movies since last date, used for hybrid rebuild
    SELECT DISTINCT a.actor_id
    FROM actors a
    INNER JOIN actorstomoviesjoin am ON a.actor_id = am.actor_id
    INNER JOIN movies m ON am.movie_id = m.movie_id
    WHERE m.release_date > @date;
END
GO


CREATE PROCEDURE [cosmos].[GetNewMoviesDirectors]
    @date datetime
AS
BEGIN
    -- Directors from new movies since last date, used for hybrid rebuild
    SELECT DISTINCT d.director_id
    FROM directors d
    INNER JOIN directorstomoviesjoin dm ON d.director_id = dm.director_id
    INNER JOIN movies m ON dm.movie_id = m.movie_id
    WHERE m.release_date > @date;
END
GO