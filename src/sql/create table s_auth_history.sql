CREATE table STV2024031246__DWH.s_auth_history
(
   hk_l_user_group_activity int CONSTRAINT fk_auth_hist_uga references STV2024031246__DWH.l_user_group_activity(hk_l_user_group_activity),
   user_id_from int,
   event varchar(10) ,
   event_dt timestamp,
   load_dt timestamp not null,
   load_src varchar(20)
);