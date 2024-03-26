INSERT INTO STV2024031246__DWH.l_user_group_activity(
        hk_l_user_group_activity,
        hk_user_id,
        hk_group_id,
        load_dt,
        load_src)
SELECT DISTINCT hash(hu.hk_user_id, hg.hk_group_id),
       hu.hk_user_id,
       hg.hk_group_id,
       now() as load_dt,
       's3' as load_src
FROM STV2024031246__STAGING.group_log as gl
LEFT JOIN STV2024031246__DWH.h_users hu on hu.user_id = gl.user_id
LEFT JOIN STV2024031246__DWH.h_groups hg on hg.group_id = gl.group_id
WHERE hash(hu.hk_user_id, hg.hk_group_id) NOT IN (SELECT hk_l_user_group_activity FROM STV2024031246__DWH.l_user_group_activity);