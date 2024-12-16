WITH film_with_ratings AS (
    SELECT 
        film_id,
        title,
        release_date,
        price,
        rating,
        user_rating,
        CASE
            WHEN user_rating >= 4.5 THEN 'Excellent'
            WHEN user_rating >= 4.0 THEN 'Good'
            WHEN user_rating >= 3.0 THEN 'Average'
            ELSE 'Poor'
        END AS rating_category
    
    FROM {{ ref('films') }}
)

films_with_actors AS (
    SELECT 
        f.film_id,
        f.title,
        STING_AGG(a.actor_name, ', ') AS actors
    FROM {{ ref('film') }} f
    LEFT JOIN {{ ref('film_actors') }} fa ON f.film_id = fa.film_id
    LEFT JOIN {{ ref('actors') }} a ON fa.actor_id = a.actor_id
    GROUP BY f.film_id, f.title
)

SELECT 
    fwf.*
    fwa.actors
FROM film_with_ratings fwf
LEFT JOIN films_with_actors fwa ON fwf.film_id = fwa.film_id;