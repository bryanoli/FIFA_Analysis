WITH cleaned_data AS (
  SELECT
    player,
    country,
    club,
    CAST(REGEXP_REPLACE(value, r'[$,.]', '') AS FLOAT64) AS cleaned_value,
    height,
    weight,
    age,
    ball_control,
    dribbling,
    marking,
    slide_tackle,
    stand_tackle,
    aggression,
    reactions,
    att_position,
    interceptions,
    vision,
    composure,
    crossing,
    short_pass,
    long_pass,
    acceleration,
    stamina,
    strength,
    balance,
    sprint_speed,
    agility,
    jumping,
    heading,
    shot_power,
    finishing,
    long_shots,
    curve,
    fk_acc,
    penalties,
    volleys,
    gk_positioning,
    gk_diving,
    gk_handling,
    gk_kicking,
    gk_reflexes
  FROM
    `premier-predict-417919.ucl_data.updated_types`
  WHERE
    player NOT LIKE '%�%' -- Filter out entries with encoding errors
),
normalized_data AS (
  SELECT
    player,
    country,
    club,
    cleaned_value,
    height,
    weight,
    age,
    ball_control,
    dribbling,
    marking,
    slide_tackle,
    stand_tackle,
    aggression,
    reactions,
    att_position,
    interceptions,
    vision,
    composure,
    crossing,
    short_pass,
    long_pass,
    acceleration,
    stamina,
    strength,
    balance,
    sprint_speed,
    agility,
    jumping,
    heading,
    shot_power,
    finishing,
    long_shots,
    curve,
    fk_acc,
    penalties,
    volleys,
    gk_positioning,
    gk_diving,
    gk_handling,
    gk_kicking,
    gk_reflexes,
    -- Normalize attributes here...
    (vision - MIN(vision) OVER ()) / (MAX(vision) OVER () - MIN(vision) OVER ()) AS normalized_vision,
    (finishing - MIN(finishing) OVER ()) / (MAX(finishing) OVER () - MIN(finishing) OVER ()) AS normalized_finishing,
    (long_shots - MIN(long_shots) OVER ()) / (MAX(long_shots) OVER () - MIN(long_shots) OVER ()) AS normalized_long_shots,
    (shot_power - MIN(shot_power) OVER ()) / (MAX(shot_power) OVER () - MIN(shot_power) OVER ()) AS normalized_shot_power,
    (curve - MIN(curve) OVER ()) / (MAX(curve) OVER () - MIN(curve) OVER ()) AS normalized_curve,
    (fk_acc - MIN(fk_acc) OVER ()) / (MAX(fk_acc) OVER () - MIN(fk_acc) OVER ()) AS normalized_fk_acc,
    (penalties - MIN(penalties) OVER ()) / (MAX(penalties) OVER () - MIN(penalties) OVER ()) AS normalized_penalties,
    (volleys - MIN(volleys) OVER ()) / (MAX(volleys) OVER () - MIN(volleys) OVER ()) AS normalized_volleys,
    (sprint_speed - MIN(sprint_speed) OVER ()) / (MAX(sprint_speed) OVER () - MIN(sprint_speed) OVER ()) AS normalized_sprint_speed,
    (acceleration - MIN(acceleration) OVER ()) / (MAX(acceleration) OVER () - MIN(acceleration) OVER ()) AS normalized_acceleration
  FROM
    cleaned_data
)
SELECT DISTINCT
  player,
  country,
  club,
  cleaned_value AS value,
  (1.0 * normalized_vision + 1.5 * normalized_finishing + 1.0 * normalized_long_shots +
   1.5 * normalized_shot_power + 1.0 * normalized_curve + 1.0 * normalized_fk_acc +
   1.0 * normalized_penalties + 1.0 * normalized_volleys + 1.0 * normalized_sprint_speed +
   1.0 * normalized_acceleration) AS composite_score,
  cleaned_value / (1.0 * normalized_vision + 1.5 * normalized_finishing + 1.0 * normalized_long_shots +
           1.5 * normalized_shot_power + 1.0 * normalized_curve + 1.0 * normalized_fk_acc +
           1.0 * normalized_penalties + 1.0 * normalized_volleys + 1.0 * normalized_sprint_speed +
           1.0 * normalized_acceleration) AS value_to_performance_ratio
FROM
  normalized_data
WHERE
  player NOT LIKE '%�%' -- Filter out entries with encoding errors
ORDER BY
  value_to_performance_ratio DESC;