"""Streamlit dashboard: surging pages and per-wiki edit rate, read from Cassandra (see queries.py)."""
import os
from datetime import timedelta

import streamlit as st

import chart
import queries as q

MIN_EDITORS = int(os.environ.get("MIN_EDITORS", "2"))
REFRESH = os.environ.get("REFRESH", "30s")  # a refresh redraws the chart and closes an open tooltip; windows are a minute wide
RANGES = {"15 minutes": 15, "30 minutes": 30, "1 hour": 60, "3 hours": 180}

st.set_page_config(page_title="Wikipedia editing surges", layout="wide")


@st.cache_resource
def session():
    return q.connect()


@st.cache_data(ttl=600)
def wiki_order():
    return q.top_wikis(session(), len(chart.SLOTS))


@st.fragment(run_every=REFRESH)
def live(minutes, top_n, wikis, colors):
    s, now = session(), q.utcnow()
    since = now - timedelta(minutes=minutes)
    th = q.threshold(s)

    alerts = q.recent_alerts(s, since, now, top_n)
    busy = q.window_rows(s, wikis, since, now)
    wide = q.edit_rate(busy, wikis, since, now)
    age = q.freshness(busy, now)

    a, b, c = st.columns(3)
    a.metric("Newest edit seen", "no data" if age is None else "%d s ago" % age)
    b.metric("Edits in the last 5 minutes (selected wikis)", int(wide.tail(5).to_numpy().sum()) if len(wide) else 0)
    c.metric("Flagged windows (period)", len(q.recent_alerts(s, since, now, 10 ** 6)))

    st.subheader("Surging pages")
    if th:
        st.caption("A page-minute is flagged when it has more than %.0f edits and at least %d distinct editors "
                   "(the %.1fth percentile of multi-editor page-minutes in the archive)." % (th[0], MIN_EDITORS, th[1] * 100))
    else:
        st.warning("No thresholds yet: run the reference job (see README).")
    if len(alerts):
        show = alerts.assign(bot_share=(alerts.bot_fraction * 100).round(0))
        st.dataframe(show[["window_start", "wiki", "page", "edit_count", "distinct_editors", "bot_share", "label"]],
                     hide_index=True, use_container_width=True, column_config={
                         "window_start": st.column_config.DatetimeColumn("minute (UTC)", format="HH:mm"),
                         "edit_count": "edits", "distinct_editors": "editors",
                         "bot_share": st.column_config.NumberColumn("bots", format="%d%%")})
    else:
        st.info("Nothing was flagged in this period. The rule is strict on purpose. Below: the busiest windows with "
                "at least %d editors in the selected wikis, for reference." % MIN_EDITORS)
        near = q.contested(busy, MIN_EDITORS, top_n)
        if len(near):
            st.dataframe(near[["window_start", "wiki", "page", "edit_count", "distinct_editors", "bot_fraction"]],
                         hide_index=True, use_container_width=True, column_config={
                             "window_start": st.column_config.DatetimeColumn("minute (UTC)", format="HH:mm"),
                             "edit_count": "edits", "distinct_editors": "editors",
                             "bot_fraction": st.column_config.NumberColumn("bot share", format="%.2f")})
        else:
            st.caption("No multi-editor windows in the selected wikis either.")

    st.subheader("Edit rate per wiki")
    if wide.empty or not wikis:
        st.info("Pick at least one wiki.")
        return
    st.altair_chart(chart.rate_chart(wide, colors), use_container_width=True)
    st.caption("Each point is one complete minute; the last minute or two can still grow as late edits arrive.")
    with st.expander("Table view"):
        st.dataframe(wide.sort_index(ascending=False), use_container_width=True)


def main():
    st.title("Wikipedia editing surges")
    order = wiki_order()
    colors = {w: chart.SLOTS[i] for i, w in enumerate(order)}
    f1, f2, f3 = st.columns([1, 1, 3])
    minutes = RANGES[f1.selectbox("Period", list(RANGES), index=2)]
    top_n = f2.number_input("Top N", 3, 50, 10)
    wikis = f3.multiselect("Wikis", order, default=order[:3], max_selections=len(chart.SLOTS)) if order else []
    if not order:
        st.warning("No wiki list yet: run the reference job (see README).")
    live(minutes, int(top_n), wikis, colors)


main()
