import numpy as np
import pandas as pd
import psycopg2

dbname = 'eicu'
schema_name = 'eicu_crd'
query_schema = 'SET search_path TO ' + schema_name + ';'
con = psycopg2.connect(dbname=dbname)
print('Connected to database. Executing query.')

query = query_schema + '''
WITH minimum_hb AS
(
    SELECT patientunitstayid, MIN(labresult) AS hb_min
    FROM lab
    WHERE labname = 'Hgb'
    AND labresultoffset >= 0
    GROUP BY patientunitstayid
)
, first_hb_adm AS
(
    WITH summary AS
    (
        SELECT l.patientunitstayid, l.labresult
        , ROW_NUMBER() OVER(PARTITION BY l.patientunitstayid ORDER BY l.labresultoffset ASC) AS rk
        FROM lab l
        WHERE labname='Hgb'
    )
    SELECT s.patientunitstayid, s.labresult AS hb_first_adm
    FROM summary s
    WHERE s.rk = 1
)
, first_hb_icu AS
(
    WITH summary AS
    (
        SELECT l.patientunitstayid, l.labresult 
        , ROW_NUMBER() OVER(PARTITION BY l.patientunitstayid ORDER BY l.labresultoffset ASC) AS rk
        FROM lab l
        WHERE labname='Hgb'
        AND labresultoffset >= 0
    )
    SELECT s.patientunitstayid, s.labresult AS hb_first_icu
    FROM summary s
    WHERE s.rk = 1
)
, transfused_prbc_ICU AS
(
    SELECT patientunitstayid
    , MAX(CASE
    WHEN LOWER(treatmentstring) LIKE '%packed red%' THEN 1
    ELSE 0
    END) AS transfused_ICU
    FROM treatment
    WHERE treatmentoffset >= 0
    GROUP BY patientunitstayid
)
, transfused_prbc_prior AS
(
    SELECT patientunitstayid
    , MAX(CASE
    WHEN LOWER(treatmentstring) LIKE '%packed red%' THEN 1
    ELSE 0
    END) AS transfused_prior
    FROM treatment
    WHERE treatmentoffset < 0
    GROUP BY patientunitstayid
)
, apache AS
(
    SELECT patientunitstayid, apachescore AS apache_score
    , actualhospitalmortality AS hospital_mortality
    FROM apachepatientresult
    WHERE apacheversion = 'IVa'
)
, pressor AS
(
    SELECT patientunitstayid
    , MAX(CASE
    WHEN LOWER(treatmentstring) LIKE '%vasopressors%' THEN 1
    ELSE 0
    END) AS vasopressor_ICU
    FROM treatment
    WHERE treatmentoffset >= 0
    GROUP BY patientunitstayid
)

SELECT p.patientunitstayid, p.hospitalid, p.age, p.gender, a.apache_score
       , m.hb_min, fa.hb_first_adm, fi.hb_first_icu, t.transfused_ICU
       , tp.transfused_prior, pr.vasopressor_ICU
       , CASE
           WHEN apacheadmissiondx in ('Angina, unstable (angina interferes w/quality of life or meds are tolerated poorly)', 
           'Infarction, acute myocardial (MI)', 
           'MI admitted > 24 hrs after onset of ischemia')
           THEN 1
           ELSE 0 END AS adx_acs
       , a.hospital_mortality
FROM patient p
INNER JOIN minimum_hb m
ON p.patientunitstayid = m.patientunitstayid
INNER JOIN transfused_prbc_ICU t
ON p.patientunitstayid = t.patientunitstayid
LEFT OUTER JOIN first_hb_adm fa
ON p.patientunitstayid = fa.patientunitstayid
LEFT OUTER JOIN first_hb_icu fi
ON p.patientunitstayid = fi.patientunitstayid
LEFT OUTER JOIN transfused_prbc_prior tp
ON p.patientunitstayid = tp.patientunitstayid
LEFT OUTER JOIN pressor pr
ON p.patientunitstayid = pr.patientunitstayid
INNER JOIN apache a
ON p.patientunitstayid = a.patientunitstayid;
'''

cohort = pd.read_sql_query(query, con)
print('Extraction complete. Cleaning.')

cohort.loc[cohort.transfused_icu == 0, 'hb_post_transfuse'] = np.nan

# Some simple data cleaning
# Remove people missing APACHE IVa score
cohort = cohort.loc[cohort.apache_score >= 0, :]

# convert age to numerical from string, switching > 89 to median of 93
cohort.loc[cohort.age == '> 89', 'age'] = 93
cohort.loc[cohort.age == '', 'age'] = np.nan
cohort.age = cohort.age.astype(float)

# code gender as binary
male_gender = (cohort.gender == 'Male').astype(int)
cohort.loc[:, 'gender'] = male_gender
cohort = cohort.rename({'gender' : 'male_gender'}, axis=1)

# code mortality as binary
hospital_mortality = (cohort.hospital_mortality == 'EXPIRED').astype(int)
cohort.hospital_mortality = hospital_mortality

# set the index to unique ICU stay identifier
cohort = cohort.set_index('patientunitstayid')

cohort.to_csv('./hb_rdd.csv', index=True, sep=',')
print('Done. Saved to hb_rdd.csv in the local directory.')
