create or replace procedure seed_mixed(n int) language plpgsql as $$
begin
  delete from harvest_task_queue; delete from harvest_workflow_executions;
  insert into harvest_task_queue(queue_name, task_type, activity_name, input, priority, scheduled_at)
    select 'q0', case when g % 2 = 0 then 'workflow' else 'activity' end, case when g % 2 = 0 then null else 'noop' end,
           jsonb_build_object('n', g), 0, now() - make_interval(secs => 50000 - g)
    from generate_series(1, n) g;
  insert into harvest_task_queue(queue_name, task_type, activity_name, input, state, attempt, worker_id, started_at, concurrency_key, concurrency_cap)
    values ('q0', 'activity', 'noop', '{}', 'RUNNING', 1, 'holder', now(), 'k', 1);
  insert into harvest_task_queue(queue_name, task_type, activity_name, input, priority, scheduled_at, concurrency_key, concurrency_cap)
    values ('q0', 'activity', 'noop', '{"n":0}', 9, now() - interval '90000 s', 'k', 1);
end $$;
