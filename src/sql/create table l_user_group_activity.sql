CREATE table STV2024031246__DWH.l_user_group_activity
(
   hk_l_user_group_activity int PRIMARY KEY,
   hk_user_id int not null CONSTRAINT fk_uga_users_hk_user_id references STV2024031246__DWH.h_users(hk_user_id),
   hk_group_id int not null CONSTRAINT fk_uga_groups_hk_group_id references STV2024031246__DWH.h_groups(hk_group_id),
   load_dt timestamp not null,
   load_src varchar(20)
);




