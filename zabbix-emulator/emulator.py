import copy
import event_data

SAMPLE_EVENT_PERIOD = 100
assert len(event_data.result) >= SAMPLE_EVENT_PERIOD, "# of data: %s" % len(event_data.result)

def generate_event(eventid):
    idx = eventid % SAMPLE_EVENT_PERIOD
    base_event = copy.copy(event_data.result[idx])

    # TODO: Change "objectid" that means change  priority, state, hosts etc.
    return base_event

def get_num_events(eventid):
    return eventid + SAMPLE_EVENT_PERIOD - 1
