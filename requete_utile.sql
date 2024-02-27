select count(*) from (
    select BUSINESS_ID, NAME, STARS, REVIEW_COUNT, IS_OPEN 
    from BUSINESS 
    WHERE BUSINESS_ID IS NOT NULL
    OR NAME IS NOT NULL
    OR STARS IS NOT NULL
    OR REVIEW_COUNT IS NOT NULL
    OR IS_OPEN IS NOT NULL
  group by BUSINESS_ID, NAME, STARS, REVIEW_COUNT, IS_OPEN
  );

select count(*) from (
    select REVIEW_ID, BUSINESS_ID,USER_ID,'DATE', STARS, USEFUL, COOL
    from FACT_REVIEW
    WHERE REVIEW_ID IS NOT NULL
    OR BUSINESS_ID IS NOT NULL
    OR USER_ID IS NOT NULL
    OR 'DATE' IS NOT NULL
    OR STARS IS NOT NULL
    OR COOL IS NOT NULL
  group by REVIEW_ID, BUSINESS_ID,USER_ID,'DATE', STARS, USEFUL, COOL
    );


select count(*) from (
    select bu.BUSINESS_ID, re.BUSINESS_ID, re.REVIEW_ID
    FROM BUSINESS bu RIGHT OUTER JOIN FACT_REVIEW re
    ON bu.BUSINESS_ID = re.BUSINESS_ID
);


select bu.BUSINESS_ID, re.REVIEW_ID FROM BUSINESS bu, FACT_REVIEW re WHERE bu.BUSINESS_ID = re.BUSINESS_ID AND bu.BUSINESS_ID = 'lcarrupLCJ-a9LGkTUtMIw';

WITH user_max_cate as 
(
    SELECT  r.USER_ID as user_id,c.category as cate,count(c.category) As count_cate 
    FROM FACT_REVIEW r, CATEGORY c 
    WHERE r.BUSINESS_ID = c.BUSINESS_ID 
    Group By r.USER_ID,c.CATEGORY 
    ORDER BY r.USER_ID
)

SELECT distinct user_max_cate.user_id, user_max_cate.cate, user_max_cate.count_cate,
first_value(user_max_cate.user_id) over (partition by  user_max_cate.cate  order by user_max_cate.user_id asc)
FROM user_max_cate 
INNER JOIN(
  select user_id, MAX(count_cate) as max_cat
  FROM user_max_cate
  group by user_id
) ap 
on user_max_cate.user_id = ap.user_id
WHERE user_max_cate.count_cate = max_cat;



select * from CHECKIN where ROWNUM <= 10;


select BUSINESS_ID, "DATE" from CHECKIN; 

SELECT BUSINESS_ID, AVG(ABS(TO_DATE(FIRST_DATE,'yyyy/mm/dd hh24:mi:ss')-TO_DATE(PREV_DATE,'yyyy/mm/dd hh24:mi:ss'))) as interval_moyen from (
  SELECT BUSINESS_ID, "DATE" as FIRST_DATE,
      LAG("DATE", 1, 0) OVER (PARTITION BY BUSINESS_ID ORDER BY BUSINESS_ID,"DATE") AS PREV_DATE
    FROM CHECKIN
    where
    ROWNUM <= 100
  )
  WHERE PREV_DATE <> '0'
  GROUP BY BUSINESS_ID;


CREATE VIEW CHECKIN_FORMATED as (SELECT BUSINESS_ID, TO_DATE("DATE",'yyyy/mm/dd hh24:mi:ss') as DATE_VALUE from CHECKIN);

SELECT BUSINESS_ID,  as DATE_VALUE from CHECKIN WHERE ROWNUM <= 5;


SELECT c.BUSINESS_ID, TRIM(c.CATEGORY) as FOOD_CATEGORY FROM CATEGORY c INNER JOIN FOOD_CATEGORY fc on TRIM(c.CATEGORY) = TRIM(fc.FOOD_CATEGORY);

select count(*) from (
  select distinct BUSINESS_ID from FOOD_FILTERED_CATEGORY
);

SELECT COUNT(*) as total FROM (
SELECT distinct b.BUSINESS_ID FROM BUSINESS b INNER JOIN FOOD_FILTERED_CATEGORY ffc on b.BUSINESS_ID = ffc.BUSINESS_ID 
);

select count(BUSINESS_ID) from CATEGORY;

select * from FOOD_BUSINESS WHERE ROWNUM<=1;

-- VUE DES CATEGORY FILTREE
CREATE VIEW FOOD_FILTERED_CATEGORY as (SELECT c.BUSINESS_ID, TRIM(c.CATEGORY) as FOOD_CATEGORY FROM CATEGORY c INNER JOIN FOOD_CATEGORY fc on TRIM(c.CATEGORY) = TRIM(fc.FOOD_CATEGORY));

-- VUE DES BUSINESS FILTREE PAR LES CATEGORY
create view FOOD_BUSINESS as (SELECT distinct b.BUSINESS_ID, b.NAME, b.STARS, b.REVIEW_COUNT, b.IS_OPEN FROM BUSINESS b INNER JOIN FOOD_FILTERED_CATEGORY ffc on b.BUSINESS_ID = ffc.BUSINESS_ID);


CREATE MATERIALIZED VIEW FAIT_REVIEW  AS
WITH review_category AS (
    SELECT
        r.REVIEW_ID,
        r.BUSINESS_ID,
        r."DATE",
        r.STARS,
        r.USER_ID,
        c.FOOD_CATEGORY as CATEGORY,
        COUNT(c.FOOD_CATEGORY) AS Ref_Category,
        RANK() OVER (PARTITION BY r.USER_ID ORDER BY COUNT(c.FOOD_CATEGORY) DESC) AS n_Rank
    FROM
        FACT_REVIEW r
    INNER JOIN
        FOOD_FILTERED_CATEGORY c ON r.BUSINESS_ID = c.BUSINESS_ID
    GROUP BY
        r.REVIEW_ID,
        r.BUSINESS_ID,
        r."DATE",
        r.STARS,
        r.USER_ID,
        c.FOOD_CATEGORY
)
SELECT
        rc.REVIEW_ID,
        rc.BUSINESS_ID,
        rc."DATE",
        rc.STARS,
        rc.USER_ID,
        rc.Ref_Category,
        rc.CATEGORY AS client_preference
FROM
    review_category rc
WHERE
    rc.n_Rank = 1