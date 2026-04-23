"""
Stateful sessionization using mapGroupsWithState.
Groups pings by vehicle_id and tracks each corridor run as a session.
"""
# TODO: implement mapGroupsWithState session logic
# State: per vehicle_id, track run_start_ts, last_ping_ts, stop_sequence, pace
# Timeout: if no ping for >5 min, close the session (is_complete=True)
