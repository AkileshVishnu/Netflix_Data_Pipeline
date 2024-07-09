{{ config(
    pre_hook="ALTER SESSION SET QUOTED_IDENTIFIERS_IGNORE_CASE = true"
) }}

{{ config(
    tags=[var('TAG_DIMENSION')]
) }}

SELECT 
* 
FROM {{ source('source_data', 'Credits') }}
WHERE ROLE IN ('ACTOR','DIRECTOR')
