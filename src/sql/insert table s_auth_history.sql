INSERT INTO STV2024031246__DWH.s_auth_history(hk_l_user_group_activity, user_id_from, event, event_dt, load_dt, load_src)       
SELECT ga.hk_l_user_group_activity,
       gl.user_id_from,
       gl.event,
       gl.event_datetime,
       now() as load_dt,
       's3' as load_src
FROM STV2024031246__STAGING.group_log as gl
LEFT JOIN STV2024031246__DWH.h_groups as hg on gl.group_id = hg.group_id
LEFT JOIN STV2024031246__DWH.h_users as hu on gl.user_id = hu.user_id
LEFT JOIN STV2024031246__DWH.l_user_group_activity as ga on hg.hk_group_id = ga.hk_group_id and hu.hk_user_id = ga.hk_user_id;