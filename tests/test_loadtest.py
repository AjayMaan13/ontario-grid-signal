from loadtest.summary import read, summarize


def test_a_single_replica_run_reports_no_scaling():
    samples = [(0, 1, 58000), (10, 1, 52000), (20, 1, 46000), (540, 1, 0), (550, 1, 0)]
    result = summarize(samples)
    assert result["peak_lag_events"] == 58000 and result["drain_seconds"] == 540
    assert result["events_per_second"] == round(58000 / 540, 1)
    assert result["max_replicas"] == 1 and result["first_scaled_up_at_seconds"] is None and result["back_to_one_replica_at_seconds"] is None


def test_a_scaled_run_reports_when_it_scaled_up_and_back_in():
    samples = [(0, 1, 58000), (15, 1, 55000), (30, 3, 52000), (45, 6, 45000), (120, 6, 20000), (230, 6, 0),
               (300, 6, 0), (360, 3, 0), (420, 1, 0)]
    result = summarize(samples)
    assert result["max_replicas"] == 6 and result["first_scaled_up_at_seconds"] == 30
    assert result["drain_seconds"] == 230 and result["back_to_one_replica_at_seconds"] == 420


def test_a_run_that_never_drained_has_no_drain_time_or_rate():
    result = summarize([(0, 1, 58000), (10, 2, 57000), (20, 2, 56000)])
    assert result["drain_seconds"] is None and result["events_per_second"] is None


def test_the_csv_the_script_writes_can_be_read_back(tmp_path):
    path = tmp_path / "run.csv"
    path.write_text("elapsed_s,replicas,lag\n0,1,100\n10,2,40\n20,2,0\n")
    assert read(path) == [(0, 1, 100), (10, 2, 40), (20, 2, 0)]
