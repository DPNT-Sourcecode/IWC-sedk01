from __future__ import annotations

from .utils import call_dequeue, call_enqueue, call_size, iso_ts, run_queue


def test_enqueue_size_dequeue_flow() -> None:
    run_queue([
        call_enqueue("companies_house", 1, iso_ts(delta_minutes=0)).expect(1),
        call_size().expect(1),
        call_dequeue().expect("companies_house", 1),
    ])



def test_deduplication_keeps_earliest_timestamp_via_run_queue() -> None:
    run_queue(
        [
            call_enqueue("bank_statements", 1, iso_ts(delta_minutes=5)).expect(1),
            call_enqueue("bank_statements", 1, iso_ts(delta_minutes=0)).expect(1),
            call_size().expect(1),
            call_dequeue().expect("bank_statements", 1),
        ]
    )


def test_dependency_resolution_orders_dependencies_before_provider_via_run_queue() -> None:
    run_queue(
        [
            # credit_check depends on companies_house -> enqueue should return size 2
            call_enqueue("credit_check", 2, iso_ts(delta_minutes=0)).expect(2),
            call_dequeue().expect("companies_house", 2),
            call_dequeue().expect("credit_check", 2),
        ]
    )


def test_rule_of_three_promotes_user_tasks_over_others_via_run_queue() -> None:
    run_queue(
        [
            call_enqueue("id_verification", 1, iso_ts(delta_minutes=10)).expect(1),
            call_enqueue("companies_house", 1, iso_ts(delta_minutes=11)).expect(2),
            call_enqueue("id_verification", 1, iso_ts(delta_minutes=12)).expect(3),
            # another user's earlier task
            call_enqueue("id_verification", 2, iso_ts(delta_minutes=9)).expect(4),
            # promoted user's task should be dequeued first
            call_dequeue().expect("id_verification", 1),
        ]
    )


def test_bank_global_deprioritisation_when_user_has_fewer_than_three_tasks_via_run_queue() -> None:
    run_queue(
        [
            call_enqueue("bank_statements", 1, iso_ts(delta_minutes=0)).expect(1),
            call_enqueue("id_verification", 1, iso_ts(delta_minutes=1)).expect(2),
            call_enqueue("companies_house", 2, iso_ts(delta_minutes=2)).expect(3),
            call_dequeue().expect("id_verification", 1),
            call_dequeue().expect("companies_house", 2),
            call_dequeue().expect("bank_statements", 1),
        ]
    )


def test_bank_per_user_deprioritisation_when_user_has_three_tasks_via_run_queue() -> None:
    run_queue(
        [
            call_enqueue("bank_statements", 1, iso_ts(delta_minutes=0)).expect(1),
            call_enqueue("id_verification", 1, iso_ts(delta_minutes=2)).expect(2),
            call_enqueue("companies_house", 1, iso_ts(delta_minutes=3)).expect(3),
            # user1 bank should be scheduled after their other tasks
            call_dequeue().expect("id_verification", 1),
            call_dequeue().expect("companies_house", 1),
            call_dequeue().expect("bank_statements", 1),
        ]
    )


def test_time_sensitive_bank_allows_bank_to_advance_but_not_skip_older_via_run_queue() -> None:
    run_queue(
        [
            # create internal age >= 5 minutes: oldest 0, newest 7 -> 7 minutes
            call_enqueue("id_verification", 1, iso_ts(delta_minutes=0)).expect(1),
            call_enqueue("bank_statements", 2, iso_ts(delta_minutes=1)).expect(2),
            call_enqueue("companies_house", 3, iso_ts(delta_minutes=7)).expect(3),
            # id_verification (oldest) must still be first, bank may come before companies_house
            call_dequeue().expect("id_verification", 1),
            call_dequeue().expect("bank_statements", 2),
            call_dequeue().expect("companies_house", 3),
        ]
    )


