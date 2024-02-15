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