WITH user_group_messages as (SELECT hg.hk_group_id,
	                                count(DISTINCT lum.hk_user_id) as cnt_users_in_group_with_messages
                             FROM STV2024031246__DWH.h_groups hg
                             LEFT JOIN STV2024031246__DWH.l_groups_dialogs lgd on lgd.hk_group_id = hg.hk_group_id
                             LEFT JOIN STV2024031246__DWH.l_user_message lum on lgd.hk_message_id = lum.hk_message_id
                             GROUP BY hg.hk_group_id), user_group_log as (SELECT luga.hk_group_id,
	                                                                             count(DISTINCT luga.hk_user_id) cnt_added_users
                                                                          FROM STV2024031246__DWH.l_user_group_activity luga
                                                                          WHERE luga.hk_l_user_group_activity in (SELECT sah.hk_l_user_group_activity
	                                                                                                              FROM STV2024031246__DWH.s_auth_history sah
	                                                                                                              WHERE event = 'add')
	                                                                      AND luga.hk_group_id in (SELECT hg.hk_group_id
	                                                                                               FROM STV2024031246__DWH.h_groups hg
	                                                                                               ORDER BY hg.registration_dt
	                                                                                               LIMIT 10)
                                                                          GROUP BY luga.hk_group_id)
SELECT ugl.hk_group_id,
	   ugl.cnt_added_users,
	   ugm.cnt_users_in_group_with_messages,
	   round(ugm.cnt_users_in_group_with_messages / ugl.cnt_added_users, 2) as group_conversion
FROM user_group_log ugl
LEFT JOIN user_group_messages ugm on ugm.hk_group_id = ugl.hk_group_id
ORDER BY group_conversion DESC;